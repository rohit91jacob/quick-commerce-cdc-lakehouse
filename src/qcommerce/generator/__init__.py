"""Seeded, deterministic simulator of a quick-commerce (10-minute delivery) operation.

The simulator is the "OLTP application": it emits transactions of INSERT/UPDATE/DELETE operations
against the ``commerce`` schema, which Debezium then captures. The simulation core
(:mod:`qcommerce.generator.engine`) is pure Python with no database access, so its invariants can
be unit-tested; :mod:`qcommerce.generator.sink` applies the transactions to Postgres.
"""
