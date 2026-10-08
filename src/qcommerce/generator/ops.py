"""Database-agnostic representation of the simulator's writes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Insert:
    table: str
    row: dict[str, Any]


@dataclass(frozen=True)
class Update:
    table: str
    key: dict[str, Any]
    values: dict[str, Any]


@dataclass(frozen=True)
class Delete:
    table: str
    key: dict[str, Any]


@dataclass(frozen=True)
class Ddl:
    """A schema change; applied by the sink through the migration runner."""

    migration_version: int


Op = Insert | Update | Delete | Ddl


@dataclass
class Txn:
    """One business transaction (applied atomically)."""

    at: datetime
    label: str
    ops: list[Op] = field(default_factory=list)

    def insert(self, table: str, **row: Any) -> None:
        self.ops.append(Insert(table, row))

    def update(self, table: str, key: dict[str, Any], **values: Any) -> None:
        self.ops.append(Update(table, key, values))

    def delete(self, table: str, **key: Any) -> None:
        self.ops.append(Delete(table, key))


def fingerprint(txns: list[Txn]) -> str:
    """Stable digest of a transaction stream (used to prove determinism)."""
    digest = hashlib.sha256()
    for txn in txns:
        digest.update(txn.at.isoformat().encode())
        digest.update(txn.label.encode())
        for op in txn.ops:
            digest.update(json.dumps(_as_jsonable(op), sort_keys=True, default=str).encode())
    return digest.hexdigest()


def _as_jsonable(op: Op) -> dict[str, Any]:
    if isinstance(op, Insert):
        return {"i": op.table, "r": op.row}
    if isinstance(op, Update):
        return {"u": op.table, "k": op.key, "v": op.values}
    if isinstance(op, Delete):
        return {"d": op.table, "k": op.key}
    return {"ddl": op.migration_version}
