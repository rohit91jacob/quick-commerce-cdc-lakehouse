-- V002: riders can now be tipped at the door.
--
-- An additive, nullable column: the canonical "online" schema change. Debezium picks it up
-- from the relation message on the next change to the table, the writer evolves the bronze
-- and silver Iceberg tables, and the dbt staging model exposes it once it exists.
ALTER TABLE commerce.orders ADD COLUMN tip_amount numeric(8,2) CHECK (tip_amount IS NULL OR tip_amount >= 0);
