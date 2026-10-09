"""Generator entry points: ``seed`` (reference data) and ``run`` (backfill, then live)."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import psycopg

from qcommerce import log
from qcommerce.db import migrate as migrations
from qcommerce.generator.engine import Simulation
from qcommerce.generator.sink import GeneratorControl, PostgresSink
from qcommerce.generator.world import build_world, load_world
from qcommerce.settings import Settings

logger = log.get(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def is_seeded(conn: psycopg.Connection) -> bool:
    return conn.execute("SELECT EXISTS (SELECT 1 FROM commerce.cities)").fetchone()[0]


def seed(settings: Settings) -> bool:
    """Insert the reference data (cities, stores, catalogue, inventory, riders, customers). Idempotent."""
    with psycopg.connect(settings.pg.dsn()) as conn:
        if is_seeded(conn):
            logger.info("reference data already present; nothing to seed")
            return False
        _, txns = build_world(settings.generator, _now())
        sink = PostgresSink(conn)
        sink.apply_all(txns)
        logger.info("seeded reference data", extra={"txns": sink.applied_txns, "rows": sink.applied_ops})
        return True


def run(
    settings: Settings,
    *,
    backfill_hours: float = 0.0,
    live_minutes: float | None = 0.0,
    rate_multiplier: float = 1.0,
    schema_change_after_minutes: float | None = None,
    catch_up_hours: float | None = None,
) -> dict[str, int]:
    """Simulate ``backfill_hours`` of history as fast as possible, then run live in wall-clock time.

    ``live_minutes=None`` runs forever. ``schema_change_after_minutes`` applies migration V002 that many
    (live) minutes in, demonstrating an online schema change flowing through the pipeline.
    """
    cfg = settings.generator
    with (
        psycopg.connect(settings.pg.dsn()) as conn,
        psycopg.connect(settings.pg.dsn(), autocommit=True) as ctl,
    ):
        if not is_seeded(conn):
            raise SystemExit("no reference data: run `qc generator seed` first")
        world = load_world(conn, cfg)
        has_orders = conn.execute("SELECT EXISTS (SELECT 1 FROM commerce.orders)").fetchone()[0]
        conn.commit()
        if catch_up_hours:
            # Scheduled refreshes: simulate the gap since the last order (capped), as fast as possible.
            last = conn.execute("SELECT max(placed_at) FROM commerce.orders").fetchone()[0]
            gap_hours = (_now() - last).total_seconds() / 3600 if last else catch_up_hours
            backfill_hours = max(0.0, min(catch_up_hours, gap_hours))
            has_orders = False
            logger.info("catch-up", extra={"last_order_at": str(last), "hours": round(backfill_hours, 2)})
        if backfill_hours and has_orders:
            logger.warning("orders already exist; skipping backfill and continuing live")
            backfill_hours = 0.0
        start = _now() - timedelta(hours=backfill_hours)
        # A distinct, reproducible RNG stream per run (keyed by how much history already exists).
        sim = Simulation(
            world, cfg, start, rate_multiplier=rate_multiplier, rng_stream=2 + world.ids.peek("orders")
        )

        def apply_migration(version: int) -> None:
            migrations.migrate(settings.pg, target=version)

        sink = PostgresSink(conn, txn_batch=cfg.backfill_txn_batch, migrate=apply_migration)
        control = GeneratorControl(ctl)
        _recover(conn, sim, sink)

        if backfill_hours:
            logger.info("backfill started", extra={"from": start.isoformat(), "hours": backfill_hours})
            cursor = start
            while cursor < _now() - timedelta(seconds=1):
                cursor = min(cursor + timedelta(minutes=15), _now())
                while control.poll(every_s=2.0):
                    time.sleep(0.5)
                for txn in sim.advance(cursor):
                    sink.apply(txn)
                sink.flush()
            logger.info(
                "backfill finished", extra={"orders": sim.stats["orders_placed"], "txns": sink.applied_txns}
            )

        sink.txn_batch = 1
        live_started = time.monotonic()
        schema_changed = world.schema_version >= 2
        logger.info("live mode", extra={"minutes": live_minutes, "rate_multiplier": rate_multiplier})
        while live_minutes is None or time.monotonic() - live_started < live_minutes * 60:
            if control.poll(every_s=1.0):
                time.sleep(0.5)
                continue
            if (
                not schema_changed
                and schema_change_after_minutes is not None
                and time.monotonic() - live_started >= schema_change_after_minutes * 60
            ):
                sink.apply(sim.schema_change())
                schema_changed = True
            for txn in sim.advance(_now()):
                sink.apply(txn)
            sink.flush()
            time.sleep(0.25)
        sink.flush()
    summary = {k: v for k, v in sim.stats.items() if not k.startswith("txn:")}
    summary["transactions"] = sink.applied_txns
    summary["operations"] = sink.applied_ops
    logger.info("generator finished", extra=summary)
    return summary


def _recover(conn: psycopg.Connection, sim: Simulation, sink: PostgresSink) -> None:
    """Cancel orders and close shifts left in flight by a previous (crashed or stopped) process."""
    rows = conn.execute(
        "SELECT o.order_id, o.store_id, o.total, p.payment_id, p.status, "
        "       coalesce((SELECT sum(r.amount) FROM commerce.refunds r WHERE r.order_id = o.order_id), 0) "
        "FROM commerce.orders o JOIN commerce.payments p ON p.order_id = o.order_id "
        "WHERE o.status NOT IN ('delivered', 'cancelled') ORDER BY o.order_id"
    ).fetchall()
    inflight = []
    for order_id, store_id, total, payment_id, payment_status, refunded in rows:
        items = conn.execute(
            "SELECT product_id, quantity FROM commerce.order_items WHERE order_id = %s ORDER BY order_item_id",
            (order_id,),
        ).fetchall()
        inflight.append(
            {
                "order_id": order_id,
                "store_id": store_id,
                "total": total,
                "payment_id": payment_id,
                "payment_status": payment_status,
                "refunded": refunded,
                "items": items,
            }
        )
    open_shifts = conn.execute(
        "SELECT shift_id, rider_id FROM commerce.rider_shifts WHERE ended_at IS NULL ORDER BY shift_id"
    ).fetchall()
    busy = [
        r[0]
        for r in conn.execute(
            "SELECT rider_id FROM commerce.riders WHERE status <> 'offline' ORDER BY rider_id"
        ).fetchall()
    ]
    conn.commit()
    if inflight or open_shifts or busy:
        sink.apply(sim.recover(inflight, open_shifts, busy))
        sink.flush()
        logger.warning(
            "recovered in-flight work from a previous run",
            extra={"orders": len(inflight), "open_shifts": len(open_shifts), "riders": len(busy)},
        )
