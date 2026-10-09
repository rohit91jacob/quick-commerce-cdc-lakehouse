-- V003: real, public market data next to the simulated store.
--
--   market_locations / market_products / market_prices
--       Open Prices (prices.openfoodfacts.org): crowdsourced shelf prices from real shops,
--       added incrementally by `qc realdata sync` (ODbL 1.0).
--   mandi_prices
--       data.gov.in Agmarknet daily wholesale prices (optional; needs a free API key).
--   products.*
--       Catalogue provenance: real Open Food Facts products carry their barcode, and their selling
--       price follows the latest real INR shelf price once Open Prices has one.
--
-- All additive, so the change flows through CDC as an online schema change (see V002).

ALTER TABLE commerce.products
    ADD COLUMN off_code          text,
    ADD COLUMN catalogue_source  text NOT NULL DEFAULT 'simulated'
        CHECK (catalogue_source IN ('simulated', 'open_food_facts')),
    ADD COLUMN price_source      text NOT NULL DEFAULT 'simulated'
        CHECK (price_source IN ('simulated', 'open_prices')),
    ADD COLUMN nutriscore_grade  text,
    ADD COLUMN image_url         text;

CREATE TABLE commerce.market_locations (
    location_id   integer PRIMARY KEY,
    osm_name      text,
    brand         text,
    city          text,
    country_code  text,
    lat           double precision,
    lon           double precision,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL
);

CREATE TABLE commerce.market_products (
    market_product_id  integer PRIMARY KEY,
    code               text        NOT NULL,
    name               text,
    brand              text,
    category_tag       text,
    store_category     text,
    quantity           numeric(12,3),
    quantity_unit      text,
    nutriscore_grade   text,
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL
);
CREATE INDEX ON commerce.market_products (code);

CREATE TABLE commerce.market_prices (
    price_id                integer PRIMARY KEY,
    market_product_id       integer REFERENCES commerce.market_products,
    location_id             integer REFERENCES commerce.market_locations,
    product_code            text,
    category_tag            text,
    price                   numeric(14,2) NOT NULL CHECK (price > 0),
    currency                text          NOT NULL CHECK (length(currency) = 3),
    is_discounted           boolean       NOT NULL,
    price_without_discount  numeric(14,2),
    price_per               text,
    observed_on             date          NOT NULL,
    source_created_at       timestamptz   NOT NULL,
    created_at              timestamptz   NOT NULL,
    updated_at              timestamptz   NOT NULL
);
CREATE INDEX ON commerce.market_prices (product_code, currency, observed_on);

CREATE TABLE commerce.mandi_prices (
    state         text          NOT NULL,
    district      text          NOT NULL,
    market        text          NOT NULL,
    commodity     text          NOT NULL,
    variety       text          NOT NULL,
    grade         text          NOT NULL,
    arrival_date  date          NOT NULL,
    min_price     numeric(10,2) NOT NULL,
    max_price     numeric(10,2) NOT NULL,
    modal_price   numeric(10,2) NOT NULL,
    created_at    timestamptz   NOT NULL,
    updated_at    timestamptz   NOT NULL,
    PRIMARY KEY (state, district, market, commodity, variety, grade, arrival_date)
);

-- Ingestion cursors (not captured: operational state, not business data).
CREATE TABLE ops.ingest_cursors (
    source      text PRIMARY KEY,
    cursor      text        NOT NULL,
    updated_at  timestamptz NOT NULL
);

ALTER PUBLICATION qc_cdc_pub ADD TABLE
    commerce.market_locations, commerce.market_products, commerce.market_prices, commerce.mandi_prices;
