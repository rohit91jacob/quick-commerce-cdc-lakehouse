-- V001: quick-commerce OLTP schema -- the system of record that Debezium captures.
--
-- Conventions
--   * Every table has a primary key (Debezium keys each Kafka message by it) and
--     created_at / updated_at business timestamps maintained by the application.
--   * Money is numeric(10,2); it is reconciled to the paisa against the lakehouse.
--   * Orders snapshot the delivery address and the promo code instead of holding foreign
--     keys to them, so addresses and expired promotions can be hard-deleted (which gives
--     the CDC pipeline real DELETE events to handle).

CREATE SCHEMA IF NOT EXISTS commerce;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE commerce.cities (
    city_id     smallint PRIMARY KEY,
    name        text             NOT NULL UNIQUE,
    state       text             NOT NULL,
    center_lat  double precision NOT NULL,
    center_lng  double precision NOT NULL,
    created_at  timestamptz      NOT NULL,
    updated_at  timestamptz      NOT NULL
);

CREATE TABLE commerce.dark_stores (
    store_id               integer PRIMARY KEY,
    city_id                smallint         NOT NULL REFERENCES commerce.cities,
    name                   text             NOT NULL UNIQUE,
    lat                    double precision NOT NULL,
    lng                    double precision NOT NULL,
    service_radius_km      numeric(4,2)     NOT NULL CHECK (service_radius_km > 0),
    max_concurrent_orders  smallint         NOT NULL CHECK (max_concurrent_orders > 0),
    status                 text             NOT NULL CHECK (status IN ('active', 'paused', 'closed')),
    opened_at              timestamptz      NOT NULL,
    created_at             timestamptz      NOT NULL,
    updated_at             timestamptz      NOT NULL
);

CREATE TABLE commerce.categories (
    category_id    smallint PRIMARY KEY,
    name           text        NOT NULL UNIQUE,
    is_perishable  boolean     NOT NULL,
    created_at     timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL
);

CREATE TABLE commerce.products (
    product_id     integer PRIMARY KEY,
    sku            text          NOT NULL UNIQUE,
    name           text          NOT NULL,
    brand          text          NOT NULL,
    category_id    smallint      NOT NULL REFERENCES commerce.categories,
    unit           text          NOT NULL,
    mrp            numeric(10,2) NOT NULL CHECK (mrp > 0),
    selling_price  numeric(10,2) NOT NULL CHECK (selling_price > 0 AND selling_price <= mrp),
    is_active      boolean       NOT NULL,
    created_at     timestamptz   NOT NULL,
    updated_at     timestamptz   NOT NULL
);

CREATE TABLE commerce.customers (
    customer_id   bigint PRIMARY KEY,
    city_id       smallint    NOT NULL REFERENCES commerce.cities,
    display_name  text        NOT NULL,
    signup_at     timestamptz NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL
);

CREATE TABLE commerce.customer_addresses (
    address_id   bigint PRIMARY KEY,
    customer_id  bigint           NOT NULL REFERENCES commerce.customers,
    store_id     integer          NOT NULL REFERENCES commerce.dark_stores,
    label        text             NOT NULL CHECK (label IN ('home', 'work', 'other')),
    lat          double precision NOT NULL,
    lng          double precision NOT NULL,
    is_default   boolean          NOT NULL,
    created_at   timestamptz      NOT NULL,
    updated_at   timestamptz      NOT NULL
);
CREATE INDEX ON commerce.customer_addresses (customer_id);

CREATE TABLE commerce.inventory (
    store_id           integer     NOT NULL REFERENCES commerce.dark_stores,
    product_id         integer     NOT NULL REFERENCES commerce.products,
    on_hand            integer     NOT NULL CHECK (on_hand >= 0),
    reorder_point      integer     NOT NULL CHECK (reorder_point >= 0),
    max_stock          integer     NOT NULL CHECK (max_stock > 0),
    last_restocked_at  timestamptz,
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL,
    PRIMARY KEY (store_id, product_id)
);

CREATE TABLE commerce.promotions (
    promo_id         integer PRIMARY KEY,
    code             text          NOT NULL UNIQUE,
    description      text          NOT NULL,
    discount_type    text          NOT NULL CHECK (discount_type IN ('percent', 'flat')),
    discount_value   numeric(8,2)  NOT NULL CHECK (discount_value > 0),
    max_discount     numeric(8,2)  NOT NULL CHECK (max_discount > 0),
    min_order_value  numeric(10,2) NOT NULL CHECK (min_order_value >= 0),
    starts_at        timestamptz   NOT NULL,
    ends_at          timestamptz   NOT NULL CHECK (ends_at > starts_at),
    created_at       timestamptz   NOT NULL,
    updated_at       timestamptz   NOT NULL
);

CREATE TABLE commerce.riders (
    rider_id      integer PRIMARY KEY,
    store_id      integer     NOT NULL REFERENCES commerce.dark_stores,
    display_name  text        NOT NULL,
    vehicle_type  text        NOT NULL CHECK (vehicle_type IN ('motorbike', 'ev_scooter', 'bicycle')),
    status        text        NOT NULL CHECK (status IN ('offline', 'idle', 'assigned', 'delivering', 'returning')),
    joined_at     timestamptz NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL
);

CREATE TABLE commerce.rider_shifts (
    shift_id    bigint PRIMARY KEY,
    rider_id    integer     NOT NULL REFERENCES commerce.riders,
    store_id    integer     NOT NULL REFERENCES commerce.dark_stores,
    started_at  timestamptz NOT NULL,
    ended_at    timestamptz CHECK (ended_at IS NULL OR ended_at >= started_at),
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE TABLE commerce.orders (
    order_id            bigint PRIMARY KEY,
    customer_id         bigint           NOT NULL REFERENCES commerce.customers,
    store_id            integer          NOT NULL REFERENCES commerce.dark_stores,
    address_id          bigint           NOT NULL,  -- snapshot reference; the address may be deleted later
    delivery_lat        double precision NOT NULL,
    delivery_lng        double precision NOT NULL,
    distance_km         numeric(5,2)     NOT NULL,
    status              text             NOT NULL CHECK (status IN
                            ('placed', 'accepted', 'picking', 'packed', 'out_for_delivery', 'delivered', 'cancelled')),
    promo_code          text,
    items_count         smallint         NOT NULL CHECK (items_count >= 0),
    subtotal            numeric(10,2)    NOT NULL CHECK (subtotal >= 0),
    discount            numeric(10,2)    NOT NULL CHECK (discount >= 0),
    delivery_fee        numeric(8,2)     NOT NULL CHECK (delivery_fee >= 0),
    total               numeric(10,2)    NOT NULL CHECK (total >= 0),
    payment_method      text             NOT NULL CHECK (payment_method IN ('upi', 'card', 'wallet', 'cod')),
    promised_minutes    smallint         NOT NULL,
    placed_at           timestamptz      NOT NULL,
    accepted_at         timestamptz,
    picking_started_at  timestamptz,
    packed_at           timestamptz,
    dispatched_at       timestamptz,
    delivered_at        timestamptz,
    cancelled_at        timestamptz,
    cancel_reason       text,
    created_at          timestamptz      NOT NULL,
    updated_at          timestamptz      NOT NULL
);
CREATE INDEX ON commerce.orders (store_id, placed_at);
CREATE INDEX ON commerce.orders (customer_id);

CREATE TABLE commerce.order_items (
    order_item_id               bigint PRIMARY KEY,
    order_id                    bigint        NOT NULL REFERENCES commerce.orders,
    product_id                  integer       NOT NULL REFERENCES commerce.products,
    quantity                    smallint      NOT NULL CHECK (quantity > 0),
    unit_price                  numeric(10,2) NOT NULL,
    mrp                         numeric(10,2) NOT NULL,
    line_total                  numeric(10,2) NOT NULL,
    substituted_for_product_id  integer REFERENCES commerce.products,
    created_at                  timestamptz   NOT NULL,
    updated_at                  timestamptz   NOT NULL
);
CREATE INDEX ON commerce.order_items (order_id);

CREATE TABLE commerce.order_status_history (
    status_event_id  bigint PRIMARY KEY,
    order_id         bigint      NOT NULL REFERENCES commerce.orders,
    status           text        NOT NULL,
    actor            text        NOT NULL CHECK (actor IN ('customer', 'store', 'rider', 'system')),
    occurred_at      timestamptz NOT NULL,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);
CREATE INDEX ON commerce.order_status_history (order_id);

CREATE TABLE commerce.delivery_assignments (
    assignment_id  bigint PRIMARY KEY,
    order_id       bigint       NOT NULL UNIQUE REFERENCES commerce.orders,
    rider_id       integer      NOT NULL REFERENCES commerce.riders,
    assigned_at    timestamptz  NOT NULL,
    picked_up_at   timestamptz,
    delivered_at   timestamptz,
    distance_km    numeric(5,2) NOT NULL,
    created_at     timestamptz  NOT NULL,
    updated_at     timestamptz  NOT NULL
);

CREATE TABLE commerce.payments (
    payment_id    bigint PRIMARY KEY,
    order_id      bigint        NOT NULL REFERENCES commerce.orders,
    method        text          NOT NULL CHECK (method IN ('upi', 'card', 'wallet', 'cod')),
    amount        numeric(10,2) NOT NULL CHECK (amount >= 0),
    status        text          NOT NULL CHECK (status IN
                      ('initiated', 'captured', 'failed', 'refunded', 'partially_refunded')),
    provider_ref  text          NOT NULL,
    created_at    timestamptz   NOT NULL,
    updated_at    timestamptz   NOT NULL
);
CREATE INDEX ON commerce.payments (order_id);

CREATE TABLE commerce.refunds (
    refund_id   bigint PRIMARY KEY,
    order_id    bigint        NOT NULL REFERENCES commerce.orders,
    payment_id  bigint        NOT NULL REFERENCES commerce.payments,
    amount      numeric(10,2) NOT NULL CHECK (amount > 0),
    reason      text          NOT NULL CHECK (reason IN
                    ('order_cancelled', 'items_unavailable', 'late_delivery', 'payment_reversal')),
    status      text          NOT NULL CHECK (status IN ('initiated', 'processed')),
    created_at  timestamptz   NOT NULL,
    updated_at  timestamptz   NOT NULL
);

-- ---------------------------------------------------------------------------------------
-- CDC plumbing (captured, but not business data)
-- ---------------------------------------------------------------------------------------

-- Row 1 is updated by Debezium's heartbeat.action.query, so the replication slot keeps
-- advancing even when no business table changes. Row 2 is the reconciliation fence: a
-- token written here and observed in silver proves the pipeline has caught up to that point.
CREATE TABLE commerce.cdc_heartbeat (
    id          smallint PRIMARY KEY,
    token       text        NOT NULL,
    beat_at     timestamptz NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);
INSERT INTO commerce.cdc_heartbeat VALUES
    (1, 'debezium-heartbeat', now(), now(), now()),
    (2, 'reconciliation-fence', now(), now(), now());

-- Debezium signalling table (ad-hoc incremental snapshots, see docs/runbook.md).
CREATE TABLE commerce.debezium_signal (
    id    varchar(64) PRIMARY KEY,
    type  varchar(32) NOT NULL,
    data  varchar(2048)
);

-- ---------------------------------------------------------------------------------------
-- Operational tables (NOT captured)
-- ---------------------------------------------------------------------------------------

-- Lets the reconciliation job pause the synthetic workload to compare a quiesced database.
CREATE TABLE ops.generator_control (
    id               smallint PRIMARY KEY CHECK (id = 1),
    paused           boolean     NOT NULL DEFAULT false,
    requested_by     text,
    requested_at     timestamptz,
    acknowledged_at  timestamptz,
    heartbeat_at     timestamptz
);
INSERT INTO ops.generator_control (id, paused) VALUES (1, false);

-- An explicit table list (rather than FOR TABLES IN SCHEMA) only needs table ownership,
-- not superuser. New tables must be added with ALTER PUBLICATION in their migration.
CREATE PUBLICATION qc_cdc_pub FOR TABLE
    commerce.cities, commerce.dark_stores, commerce.categories, commerce.products,
    commerce.customers, commerce.customer_addresses, commerce.inventory, commerce.promotions,
    commerce.riders, commerce.rider_shifts, commerce.orders, commerce.order_items,
    commerce.order_status_history, commerce.delivery_assignments, commerce.payments,
    commerce.refunds, commerce.cdc_heartbeat, commerce.debezium_signal;
