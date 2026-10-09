"""``qc`` command-line interface."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from qcommerce import log
from qcommerce.settings import get_settings


def _db(args: argparse.Namespace) -> int:
    from qcommerce.db import migrate

    settings = get_settings()
    if args.action == "migrate":
        applied = migrate.migrate(settings.pg, target=args.target)
        print(f"applied migrations: {applied or 'none (up to date)'}")
    else:
        for version, name, done in migrate.status(settings.pg):
            print(f"V{version:03d} {name:<28} {'applied' if done else 'pending'}")
    return 0


def _generator(args: argparse.Namespace) -> int:
    from qcommerce.generator import runner
    from qcommerce.generator.sink import GeneratorControl

    settings = get_settings()
    if args.action == "seed":
        runner.seed(settings)
    elif args.action == "run":
        summary = runner.run(
            settings,
            backfill_hours=args.backfill_hours,
            catch_up_hours=args.catch_up_hours,
            live_minutes=None if args.live_minutes < 0 else args.live_minutes,
            rate_multiplier=args.rate_multiplier,
            schema_change_after_minutes=args.schema_change_after_minutes,
        )
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        import psycopg

        with psycopg.connect(settings.pg.dsn()) as conn:
            GeneratorControl.request(conn, paused=args.action == "pause", by="cli")
        print(f"generator {args.action} requested")
    return 0


def _cdc(args: argparse.Namespace) -> int:
    from qcommerce.cdc import connector

    settings = get_settings()
    if args.action == "register":
        if args.recreate:
            connector.ConnectClient(settings.connect.url).delete(settings.connect.connector_name)
        status = connector.register(settings)
        print(json.dumps(status, indent=2))
    elif args.action == "status":
        client = connector.ConnectClient(settings.connect.url)
        print(
            json.dumps(
                {
                    "connector": client.status(settings.connect.connector_name),
                    "slot": connector.slot_lag(settings),
                },
                indent=2,
                default=str,
            )
        )
    elif args.action == "snapshot":
        signal_id = connector.request_incremental_snapshot(settings, args.tables)
        print(f"incremental snapshot requested ({signal_id}) for {', '.join(args.tables)}")
    elif args.action == "config":
        config = connector.connector_config(settings)
        config["database.password"] = "********"
        print(json.dumps(config, indent=2))
    return 0


def _writer(args: argparse.Namespace) -> int:
    from qcommerce.lakehouse import writer

    writer.run(get_settings(), available_now=args.available_now)
    return 0


def _reconcile(args: argparse.Namespace) -> int:
    from qcommerce import reconcile

    report = reconcile.run(
        get_settings(),
        mode=args.mode,
        settle_minutes=args.settle_minutes,
        timeout_s=args.timeout,
        pause_generator=not args.no_pause,
    )
    print(reconcile.format_report(report))
    if args.report:
        Path(args.report).write_text(report.to_json(), encoding="utf-8")
    return 0 if report.ok else 1


def _maintenance(args: argparse.Namespace) -> int:
    from qcommerce import maintenance

    results = maintenance.run(
        get_settings(),
        retention=args.retention,
        file_size_threshold=args.file_size_threshold,
        purge_tombstones_older_than_days=args.purge_tombstones_days,
    )
    for r in results:
        print(
            f"{r.table:<40} {r.seconds:>7.2f}s  {','.join(r.actions)}{'  ERROR: ' + r.error if r.error else ''}"
        )
    return 1 if any(r.error for r in results) else 0


def _ops(args: argparse.Namespace) -> int:
    from qcommerce import checksums, ops, trino_client
    from qcommerce.cdc import connector

    settings = get_settings()
    if args.action == "status":
        health = ops.check(settings, writer_url=args.writer_url, max_staleness_s=args.max_staleness)
        print(json.dumps(health.as_dict(), indent=2, default=str))
        return 0 if health.ok else 1
    if args.action == "checksums":
        result = checksums.layer_checksums(settings, args.schema)
        print(json.dumps({t: {"rows": n, "checksum": c} for t, (n, c) in result.items()}, indent=2))
        return 0
    if args.action == "wait":
        deadline = time.monotonic() + args.timeout
        for target in args.targets:
            if target == "trino":
                trino_client.wait_ready(settings.trino, timeout_s=max(1.0, deadline - time.monotonic()))
            elif target == "connect":
                connector.ConnectClient(settings.connect.url).wait_ready(
                    max(1.0, deadline - time.monotonic())
                )
            print(f"{target}: ready")
        return 0
    return 2


def _realdata(args: argparse.Namespace) -> int:
    from qcommerce.realdata import catalogue, sync

    if args.action == "catalogue-snapshot":
        default = Path(catalogue.__file__).with_name(catalogue.SNAPSHOT)
        print(json.dumps(catalogue.build_snapshot(Path(args.out) if args.out else default)))
        return 0
    fixtures = None
    if args.fixtures == "bundled":  # the real sample pages shipped with the package (CI, offline demos)
        from importlib import resources

        fixtures = Path(str(resources.files("qcommerce.realdata").joinpath("fixtures")))
    elif args.fixtures:
        fixtures = Path(args.fixtures)
    summary = sync.sync(
        get_settings(),
        fixtures=fixtures,
        backfill_days=args.backfill_days,
        max_pages=args.max_pages,
    )
    text = json.dumps(summary, default=str, indent=2)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    print(text)
    return 0


def _report(args: argparse.Namespace) -> int:
    from qcommerce import site

    print(site.build(get_settings(), Path(args.out)))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qc", description="Quick-commerce CDC lakehouse")
    sub = parser.add_subparsers(dest="command", required=True)

    db = sub.add_parser("db", help="OLTP schema migrations")
    db.add_argument("action", choices=["migrate", "status"])
    db.add_argument("--target", type=int, default=None, help="highest migration version to apply")
    db.set_defaults(func=_db)

    gen = sub.add_parser("generator", help="synthetic quick-commerce workload")
    gen.add_argument("action", choices=["seed", "run", "pause", "resume"])
    gen.add_argument("--backfill-hours", type=float, default=0.0)
    gen.add_argument("--live-minutes", type=float, default=0.0, help="-1 runs forever")
    gen.add_argument("--rate-multiplier", type=float, default=1.0)
    gen.add_argument(
        "--catch-up-hours",
        type=float,
        default=None,
        help="simulate the time since the last order (at most this many hours), then stop or go live",
    )
    gen.add_argument(
        "--schema-change-after-minutes",
        type=float,
        default=None,
        help="apply migration V002 (orders.tip_amount) this many live minutes in",
    )
    gen.set_defaults(func=_generator)

    cdc = sub.add_parser("cdc", help="Debezium connector")
    cdc.add_argument("action", choices=["register", "status", "snapshot", "config"])
    cdc.add_argument("--recreate", action="store_true", help="delete the connector first (keeps the slot)")
    cdc.add_argument("--tables", nargs="+", default=[], help="for `snapshot`: e.g. commerce.orders")
    cdc.set_defaults(func=_cdc)

    writer = sub.add_parser("writer", help="Spark Structured Streaming: Kafka -> Iceberg")
    writer.add_argument("action", choices=["run"])
    writer.add_argument("--available-now", action="store_true", help="drain what is in Kafka, then exit")
    writer.set_defaults(func=_writer)

    rec = sub.add_parser("reconcile", help="compare Postgres with the Iceberg silver layer")
    rec.add_argument("--mode", choices=["exact", "settled"], default="exact")
    rec.add_argument("--settle-minutes", type=float, default=15.0)
    rec.add_argument("--timeout", type=float, default=900.0)
    rec.add_argument("--no-pause", action="store_true", help="exact mode without pausing the generator")
    rec.add_argument("--report", help="write the JSON report here")
    rec.set_defaults(func=_reconcile)

    mnt = sub.add_parser("maintenance", help="Iceberg compaction / snapshot expiry / orphan cleanup")
    mnt.add_argument("action", choices=["run"])
    mnt.add_argument("--retention", default="7d")
    mnt.add_argument("--file-size-threshold", default="64MB")
    mnt.add_argument("--purge-tombstones-days", type=int, default=None)
    mnt.set_defaults(func=_maintenance)

    real = sub.add_parser("realdata", help="real public data: Open Food Facts, Open Prices, Agmarknet")
    real.add_argument("action", choices=["sync", "catalogue-snapshot"])
    real.add_argument(
        "--fixtures",
        default=None,
        help="for `sync`: read Open Prices pages from this directory (`bundled`: the packaged sample)",
    )
    real.add_argument("--backfill-days", type=float, default=14.0, help="first-run history to fetch")
    real.add_argument("--max-pages", type=int, default=150, help="Open Prices pages per run (100 rows each)")
    real.add_argument("--out", default=None, help="for `catalogue-snapshot`: output path")
    real.add_argument("--report", default=None, help="for `sync`: write the JSON summary here")
    real.set_defaults(func=_realdata)

    rpt = sub.add_parser("report", help="render the static results site from the gold marts")
    rpt.add_argument("--out", default="site")
    rpt.set_defaults(func=_report)

    ops = sub.add_parser("ops", help="health checks and utilities")
    ops.add_argument("action", choices=["status", "checksums", "wait"])
    ops.add_argument("--writer-url", default=None, help="e.g. http://localhost:9405")
    ops.add_argument("--max-staleness", type=float, default=None, help="seconds")
    ops.add_argument("--schema", default="silver", help="for `checksums`")
    ops.add_argument("--targets", nargs="+", default=["trino", "connect"], help="for `wait`")
    ops.add_argument("--timeout", type=float, default=300.0)
    ops.set_defaults(func=_ops)
    return parser


def main(argv: list[str] | None = None) -> int:
    log.configure()
    args = build_parser().parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
