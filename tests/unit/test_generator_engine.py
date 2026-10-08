"""Invariants of the simulated OLTP workload, checked by replaying its operations in memory."""

from __future__ import annotations

import itertools
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from qcommerce.generator.demand import IST
from qcommerce.generator.engine import ALLOWED_TRANSITIONS, Simulation
from qcommerce.generator.ops import Ddl, Delete, Insert, Txn, Update, fingerprint
from qcommerce.generator.world import build_world
from qcommerce.oltp import PRIMARY_KEYS
from qcommerce.settings import GeneratorSettings

START = datetime(2026, 9, 3, 0, 0, tzinfo=IST)


def small_config(seed: int = 7) -> GeneratorSettings:
    return GeneratorSettings(
        seed=seed,
        cities=1,
        stores_per_city=2,
        products=90,
        customers=300,
        riders_per_store=16,
        base_orders_per_store_hour=14.0,
    )


def simulate(cfg: GeneratorSettings, hours: float, *, schema_change_at: float | None = None) -> list[Txn]:
    world, txns = build_world(cfg, START)
    sim = Simulation(world, cfg, START)
    if schema_change_at is None:
        txns += list(sim.advance(START + timedelta(hours=hours)))
    else:
        txns += list(sim.advance(START + timedelta(hours=schema_change_at)))
        txns.append(sim.schema_change())
        txns += list(sim.advance(START + timedelta(hours=hours)))
    return txns


class Database:
    """Applies Insert/Update/Delete ops like Postgres would, failing on anything Postgres would reject."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[tuple, dict]] = {t: {} for t in PRIMARY_KEYS}
        self.op_counts: Counter[tuple[str, str]] = Counter()

    def key(self, table: str, values: dict) -> tuple:
        return tuple(values[k] for k in PRIMARY_KEYS[table])

    def apply(self, txn: Txn) -> None:
        for op in txn.ops:
            if isinstance(op, Ddl):
                continue
            rows = self.tables[op.table]
            if isinstance(op, Insert):
                key = self.key(op.table, op.row)
                assert key not in rows, f"duplicate key {op.table}{key}"
                rows[key] = dict(op.row)
                self.op_counts[(op.table, "insert")] += 1
            elif isinstance(op, Update):
                key = self.key(op.table, op.key)
                assert key in rows, f"update of missing row {op.table}{key} ({txn.label})"
                rows[key].update(op.values)
                self.op_counts[(op.table, "update")] += 1
            elif isinstance(op, Delete):
                key = self.key(op.table, op.key)
                assert key in rows, f"delete of missing row {op.table}{key}"
                del rows[key]
                self.op_counts[(op.table, "delete")] += 1
        for row in self.tables["inventory"].values():
            assert row["on_hand"] >= 0, f"negative stock after {txn.label}"


@pytest.fixture(scope="module")
def run() -> tuple[list[Txn], Database]:
    txns = simulate(small_config(), hours=80, schema_change_at=40)
    db = Database()
    for txn in txns:
        db.apply(txn)
    return txns, db


def test_same_seed_same_stream() -> None:
    assert fingerprint(simulate(small_config(seed=11), 6)) == fingerprint(simulate(small_config(seed=11), 6))


def test_different_seed_different_stream() -> None:
    assert fingerprint(simulate(small_config(seed=11), 6)) != fingerprint(simulate(small_config(seed=12), 6))


def test_stream_is_valid_for_a_relational_database(run) -> None:
    _, db = run  # the fixture already applied every op with key/existence/stock checks
    assert len(db.tables["orders"]) > 500


def test_stream_contains_real_deletes(run) -> None:
    _, db = run
    deleted = {table for (table, kind) in db.op_counts if kind == "delete"}
    assert {"order_items", "promotions", "customer_addresses", "inventory"} <= deleted


def test_order_lifecycle_follows_the_state_machine(run) -> None:
    txns, _ = run
    status: dict[int, str] = {}
    for txn in txns:
        for op in txn.ops:
            if isinstance(op, Insert) and op.table == "orders":
                status[op.row["order_id"]] = op.row["status"]
            elif isinstance(op, Update) and op.table == "orders" and "status" in op.values:
                order_id, new = op.key["order_id"], op.values["status"]
                if op.values.get("cancel_reason") == "system_recovery":
                    continue
                assert new in ALLOWED_TRANSITIONS[status[order_id]], f"{status[order_id]} -> {new}"
                status[order_id] = new
    finished = Counter(status.values())
    assert finished["delivered"] > 0 and finished["cancelled"] > 0


def test_order_money_adds_up(run) -> None:
    _, db = run
    items_by_order: dict[int, Decimal] = {}
    for item in db.tables["order_items"].values():
        assert item["line_total"] == item["unit_price"] * item["quantity"]
        items_by_order[item["order_id"]] = (
            items_by_order.get(item["order_id"], Decimal(0)) + item["line_total"]
        )
    refunds: dict[int, Decimal] = {}
    for refund in db.tables["refunds"].values():
        refunds[refund["order_id"]] = refunds.get(refund["order_id"], Decimal(0)) + refund["amount"]
    for order in db.tables["orders"].values():
        assert order["total"] == order["subtotal"] - order["discount"] + order["delivery_fee"]
        if order["items_count"]:
            assert order["subtotal"] == items_by_order[order["order_id"]]
        assert refunds.get(order["order_id"], Decimal(0)) <= order["total"]


def test_delivered_orders_have_ordered_timestamps(run) -> None:
    _, db = run
    delivered = [o for o in db.tables["orders"].values() if o["status"] == "delivered"]
    assert delivered
    for o in delivered:
        stages = [
            o["placed_at"],
            o["accepted_at"],
            o["picking_started_at"],
            o["packed_at"],
            o["dispatched_at"],
            o["delivered_at"],
        ]
        assert all(a <= b for a, b in itertools.pairwise(stages)), o["order_id"]


def test_sla_has_both_hits_and_breaches(run) -> None:
    _, db = run
    minutes = [
        (o["delivered_at"] - o["placed_at"]).total_seconds() / 60
        for o in db.tables["orders"].values()
        if o["status"] == "delivered"
    ]
    within = sum(m <= 10 for m in minutes) / len(minutes)
    assert 0.2 < within < 0.95, within


def test_tips_only_after_schema_change(run) -> None:
    txns, db = run
    ddl_at = next(t.at for t in txns if any(isinstance(op, Ddl) for op in t.ops))
    tipped = [o for o in db.tables["orders"].values() if o.get("tip_amount") is not None]
    assert tipped
    assert all(o["delivered_at"] >= ddl_at for o in tipped)


def test_deleted_inventory_rows_get_relisted_with_the_same_key(run) -> None:
    txns, _ = run
    deleted, relisted = set(), set()
    for txn in txns:
        for op in txn.ops:
            if op.__class__ is Delete and op.table == "inventory":
                deleted.add((op.key["store_id"], op.key["product_id"]))
            if op.__class__ is Insert and op.table == "inventory" and txn.label == "relisted":
                relisted.add((op.row["store_id"], op.row["product_id"]))
    assert deleted and relisted <= deleted
