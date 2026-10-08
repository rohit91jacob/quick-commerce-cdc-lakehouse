"""The simulated world: reference entities plus the mutable state the simulation needs.

``build_world`` creates a fresh world from a seed (and the bootstrap transactions that insert it),
``load_world`` reconstructs one from the database so a restarted generator can resume.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import numpy as np

from qcommerce.generator.ops import Txn
from qcommerce.generator.reference import CATEGORIES, CITIES, NEIGHBOURHOODS, VEHICLES, CategorySpec
from qcommerce.settings import GeneratorSettings

CENT = Decimal("0.01")


def money(value: float | Decimal | int) -> Decimal:
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def point_within(rng: np.random.Generator, lat: float, lng: float, radius_km: float) -> tuple[float, float]:
    """Uniformly distributed point inside a disc of ``radius_km`` around (lat, lng)."""
    d = radius_km * math.sqrt(rng.random())
    theta = 2 * math.pi * rng.random()
    dlat = d * math.cos(theta) / 111.32
    dlng = d * math.sin(theta) / (111.32 * math.cos(math.radians(lat)))
    return round(lat + dlat, 6), round(lng + dlng, 6)


def entity_rng(seed: int, kind: str, entity_id: int) -> np.random.Generator:
    """Per-entity RNG so hidden traits (propensity, popularity...) are reproducible on reload."""
    return np.random.default_rng([seed, sum(map(ord, kind)), entity_id])


@dataclass
class City:
    city_id: int
    name: str
    state: str
    lat: float
    lng: float
    rain_windows: list[tuple[datetime, datetime]] = field(default_factory=list)


@dataclass
class Store:
    store_id: int
    city_id: int
    name: str
    lat: float
    lng: float
    service_radius_km: Decimal
    max_concurrent_orders: int
    status: str
    opened_at: datetime
    demand_factor: float
    # runtime
    active_picking: int = 0
    pick_queue: deque[int] = field(default_factory=deque)
    dispatch_queue: deque[int] = field(default_factory=deque)
    idle_riders: deque[int] = field(default_factory=deque)
    customer_pool: list[int] = field(default_factory=list)


@dataclass
class Category:
    category_id: int
    spec: CategorySpec


@dataclass
class Product:
    product_id: int
    sku: str
    name: str
    brand: str
    category_id: int
    unit: str
    mrp: Decimal
    selling_price: Decimal
    is_active: bool
    popularity: float


@dataclass
class Customer:
    customer_id: int
    city_id: int
    signup_at: datetime
    propensity: float  # expected orders per week while active
    churn_at: datetime
    address_ids: list[int] = field(default_factory=list)
    default_address_id: int | None = None


@dataclass
class Address:
    address_id: int
    customer_id: int
    store_id: int
    label: str
    lat: float
    lng: float
    is_default: bool


@dataclass
class StockLevel:
    on_hand: int
    reorder_point: int
    max_stock: int


@dataclass
class Rider:
    rider_id: int
    store_id: int
    vehicle_type: str
    speed_kmh: float
    shift_start_hour: int  # IST
    shift_hours: int
    status: str = "offline"
    shift_id: int | None = None
    ending: bool = False


@dataclass
class Promotion:
    promo_id: int
    code: str
    discount_type: str
    discount_value: Decimal
    max_discount: Decimal
    min_order_value: Decimal
    starts_at: datetime
    ends_at: datetime


class IdAllocator:
    """Application-assigned surrogate keys (deterministic, unlike database sequences)."""

    def __init__(self, start: dict[str, int] | None = None) -> None:
        self._next = dict(start or {})

    def next(self, name: str) -> int:
        value = self._next.get(name, 1)
        self._next[name] = value + 1
        return value

    def peek(self, name: str) -> int:
        return self._next.get(name, 1)


@dataclass
class World:
    seed: int
    cities: dict[int, City] = field(default_factory=dict)
    stores: dict[int, Store] = field(default_factory=dict)
    categories: dict[int, Category] = field(default_factory=dict)
    products: dict[int, Product] = field(default_factory=dict)
    products_by_category: dict[int, list[int]] = field(default_factory=dict)
    customers: dict[int, Customer] = field(default_factory=dict)
    addresses: dict[int, Address] = field(default_factory=dict)
    inventory: dict[tuple[int, int], StockLevel] = field(default_factory=dict)
    riders: dict[int, Rider] = field(default_factory=dict)
    promotions: dict[int, Promotion] = field(default_factory=dict)
    ids: IdAllocator = field(default_factory=IdAllocator)
    schema_version: int = 1

    def stores_in_city(self, city_id: int) -> list[Store]:
        return [s for s in self.stores.values() if s.city_id == city_id]

    def rebuild_indexes(self) -> None:
        self.products_by_category = {}
        for product in self.products.values():
            self.products_by_category.setdefault(product.category_id, []).append(product.product_id)
        for store in self.stores.values():
            store.customer_pool = []
        for customer in self.customers.values():
            address = self.addresses.get(customer.default_address_id or -1)
            if address is not None:
                self.stores[address.store_id].customer_pool.append(customer.customer_id)


# --------------------------------------------------------------------------------------------
# Hidden (non-persisted) traits, derived from ids so a reloaded world behaves identically.
# --------------------------------------------------------------------------------------------


def _store_demand_factor(seed: int, store_id: int) -> float:
    return float(np.clip(entity_rng(seed, "store", store_id).lognormal(0.0, 0.25), 0.6, 1.8))


def _product_popularity(seed: int, product_id: int) -> float:
    # Heavy-tailed: a few SKUs (milk, bread, onions...) dominate baskets.
    return float(entity_rng(seed, "product", product_id).pareto(1.6) + 0.05)


def _customer_traits(seed: int, customer_id: int, signup_at: datetime) -> tuple[float, datetime]:
    rng = entity_rng(seed, "customer", customer_id)
    propensity = float(np.clip(rng.lognormal(math.log(1.1), 0.85), 0.08, 12.0))
    lifetime_days = float(rng.exponential(110.0)) + 3.0
    return propensity, signup_at + timedelta(days=lifetime_days)


def _rider_traits(seed: int, rider_id: int) -> tuple[str, float, int, int]:
    rng = entity_rng(seed, "rider", rider_id)
    names = [v[0] for v in VEHICLES]
    shares = np.array([v[1] for v in VEHICLES])
    vehicle = names[int(rng.choice(len(names), p=shares / shares.sum()))]
    speed = {v[0]: v[2] for v in VEHICLES}[vehicle]
    roll = rng.random()
    # morning 07-15, evening 15-23, a thin night shift 23-07, plus evening-peak part-timers 18-23
    start, hours = (7, 8) if roll < 0.35 else (15, 8) if roll < 0.75 else (23, 8) if roll < 0.85 else (18, 5)
    return vehicle, speed, start, hours


# --------------------------------------------------------------------------------------------
# Fresh world
# --------------------------------------------------------------------------------------------


def build_world(cfg: GeneratorSettings, start: datetime) -> tuple[World, list[Txn]]:
    """Create the reference data for a new simulation and the transactions that insert it."""
    rng = np.random.default_rng([cfg.seed, 1])
    world = World(seed=cfg.seed)
    created = start - timedelta(days=45)
    txns: list[Txn] = []

    txn = Txn(created, "seed:cities_stores_categories")
    for city_id, spec in enumerate(CITIES[: cfg.cities], start=1):
        world.cities[city_id] = City(city_id, spec.name, spec.state, spec.lat, spec.lng)
        txn.insert(
            "cities",
            city_id=city_id,
            name=spec.name,
            state=spec.state,
            center_lat=spec.lat,
            center_lng=spec.lng,
            created_at=created,
            updated_at=created,
        )
    store_id = 0
    for city in world.cities.values():
        hoods = NEIGHBOURHOODS[city.name]
        for i in range(cfg.stores_per_city):
            store_id += 1
            lat, lng = point_within(rng, city.lat, city.lng, 9.0)
            opened = created - timedelta(days=int(rng.integers(30, 400)))
            store = Store(
                store_id=store_id,
                city_id=city.city_id,
                name=f"{city.name} - {hoods[i % len(hoods)]}",
                lat=lat,
                lng=lng,
                service_radius_km=money(round(float(rng.uniform(1.8, 2.6)), 1)),
                max_concurrent_orders=int(rng.integers(4, 8)),
                status="active",
                opened_at=opened,
                demand_factor=_store_demand_factor(cfg.seed, store_id),
            )
            world.stores[store_id] = store
            txn.insert(
                "dark_stores",
                store_id=store_id,
                city_id=city.city_id,
                name=store.name,
                lat=lat,
                lng=lng,
                service_radius_km=store.service_radius_km,
                max_concurrent_orders=store.max_concurrent_orders,
                status="active",
                opened_at=opened,
                created_at=created,
                updated_at=created,
            )
    for category_id, spec in enumerate(CATEGORIES, start=1):
        world.categories[category_id] = Category(category_id, spec)
        txn.insert(
            "categories",
            category_id=category_id,
            name=spec.name,
            is_perishable=spec.perishable,
            created_at=created,
            updated_at=created,
        )
    txns.append(txn)

    # Products: split the catalogue across categories by demand weight.
    weights = np.array([c.spec.demand_weight for c in world.categories.values()])
    per_category = np.maximum(3, np.round(weights / weights.sum() * cfg.products)).astype(int)
    txn = Txn(created, "seed:products")
    product_id = 0
    seen_names: set[str] = set()
    for category in world.categories.values():
        spec = category.spec
        for _ in range(int(per_category[category.category_id - 1])):
            for _attempt in range(20):
                brand = spec.brands[int(rng.integers(len(spec.brands)))]
                item = spec.items[int(rng.integers(len(spec.items)))]
                unit = spec.units[int(rng.integers(len(spec.units)))]
                name = f"{brand} {item} {unit}"
                if name not in seen_names:
                    break
            else:
                continue
            seen_names.add(name)
            product_id += 1
            mrp = money(int(rng.uniform(*spec.price_range)))
            selling = money(max(1, int(float(mrp) * float(rng.uniform(0.78, 1.0)))))
            product = Product(
                product_id,
                f"SKU-{product_id:05d}",
                name,
                brand,
                category.category_id,
                unit,
                mrp,
                selling,
                True,
                _product_popularity(cfg.seed, product_id),
            )
            world.products[product_id] = product
            txn.insert(
                "products",
                product_id=product_id,
                sku=product.sku,
                name=name,
                brand=brand,
                category_id=category.category_id,
                unit=product.unit,
                mrp=mrp,
                selling_price=selling,
                is_active=True,
                created_at=created,
                updated_at=created,
            )
    txns.append(txn)

    # Inventory: most stores stock most SKUs.
    txn = Txn(created, "seed:inventory")
    for store in world.stores.values():
        for product in world.products.values():
            if rng.random() > 0.93:
                continue
            perishable = world.categories[product.category_id].spec.perishable
            max_stock = int(rng.integers(14, 40) if perishable else rng.integers(20, 75))
            level = StockLevel(
                int(rng.integers(max_stock // 2, max_stock + 1)), max(2, max_stock // 4), max_stock
            )
            world.inventory[(store.store_id, product.product_id)] = level
            txn.insert(
                "inventory",
                store_id=store.store_id,
                product_id=product.product_id,
                on_hand=level.on_hand,
                reorder_point=level.reorder_point,
                max_stock=max_stock,
                last_restocked_at=created,
                created_at=created,
                updated_at=created,
            )
    txns.append(txn)

    # Riders
    txn = Txn(created, "seed:riders")
    for store in world.stores.values():
        for _ in range(cfg.riders_per_store):
            rider_id = world.ids.next("riders")
            vehicle, speed, shift_start, shift_hours = _rider_traits(cfg.seed, rider_id)
            world.riders[rider_id] = Rider(rider_id, store.store_id, vehicle, speed, shift_start, shift_hours)
            joined = created - timedelta(days=int(rng.integers(5, 300)))
            txn.insert(
                "riders",
                rider_id=rider_id,
                store_id=store.store_id,
                display_name=f"Rider {rider_id:04d}",
                vehicle_type=vehicle,
                status="offline",
                joined_at=joined,
                created_at=created,
                updated_at=created,
            )
    txns.append(txn)

    # Long-running promotions (daily flash promos are created by the simulation).
    txn = Txn(created, "seed:promotions")
    for code, description, dtype, value, cap, minimum in (
        ("WELCOME50", "Flat 50 off on orders above 199", "flat", "50", "50", "199"),
        ("SAVE10", "10% off up to 75 on orders above 299", "percent", "10", "75", "299"),
        ("MEGA15", "15% off up to 120 on orders above 599", "percent", "15", "120", "599"),
        ("WEEKEND20", "20% off up to 100 on orders above 399", "percent", "20", "100", "399"),
    ):
        promo = Promotion(
            world.ids.next("promotions"),
            code,
            dtype,
            Decimal(value),
            Decimal(cap),
            Decimal(minimum),
            created,
            start + timedelta(days=365),
        )
        world.promotions[promo.promo_id] = promo
        txn.insert(
            "promotions",
            promo_id=promo.promo_id,
            code=code,
            description=description,
            discount_type=dtype,
            discount_value=promo.discount_value,
            max_discount=promo.max_discount,
            min_order_value=promo.min_order_value,
            starts_at=promo.starts_at,
            ends_at=promo.ends_at,
            created_at=created,
            updated_at=created,
        )
    txns.append(txn)

    # Existing customer base, signed up over the 120 days before the simulation starts.
    batch = Txn(created, "seed:customers:0")
    for i in range(cfg.customers):
        if i % 500 == 0 and batch.ops:
            txns.append(batch)
            batch = Txn(created, f"seed:customers:{i}")
        signup = start - timedelta(days=float(rng.uniform(0.5, 120.0)))
        city_id = int(rng.integers(1, len(world.cities) + 1))
        customer = new_customer(world, rng, batch, city_id, signup)
        if rng.random() < 0.25:  # a second (work) address somewhere else in the city
            stores = world.stores_in_city(city_id)
            add_address(
                world,
                rng,
                batch,
                customer,
                stores[int(rng.integers(len(stores)))],
                signup + timedelta(days=float(rng.uniform(0, 30))),
                label="work",
                default=False,
            )
    if batch.ops:
        txns.append(batch)
    world.rebuild_indexes()
    return world, txns


def new_customer(
    world: World, rng: np.random.Generator, txn: Txn, city_id: int, at: datetime, store: Store | None = None
) -> Customer:
    """Register a customer and their home address (near ``store`` if given)."""
    customer_id = world.ids.next("customers")
    propensity, churn_at = _customer_traits(world.seed, customer_id, at)
    customer = Customer(customer_id, city_id, at, propensity, churn_at)
    world.customers[customer_id] = customer
    txn.insert(
        "customers",
        customer_id=customer_id,
        city_id=city_id,
        display_name=f"Customer {customer_id:06d}",
        signup_at=at,
        created_at=at,
        updated_at=at,
    )
    if store is None:
        candidates = world.stores_in_city(city_id)
        store = candidates[int(rng.integers(len(candidates)))]
    add_address(world, rng, txn, customer, store, at, label="home", default=True)
    return customer


def add_address(
    world: World,
    rng: np.random.Generator,
    txn: Txn,
    customer: Customer,
    store: Store,
    at: datetime,
    *,
    label: str,
    default: bool,
) -> Address:
    address_id = world.ids.next("addresses")
    lat, lng = point_within(rng, store.lat, store.lng, float(store.service_radius_km) * 0.95)
    address = Address(address_id, customer.customer_id, store.store_id, label, lat, lng, default)
    world.addresses[address_id] = address
    customer.address_ids.append(address_id)
    if default:
        customer.default_address_id = address_id
    txn.insert(
        "customer_addresses",
        address_id=address_id,
        customer_id=customer.customer_id,
        store_id=store.store_id,
        label=label,
        lat=lat,
        lng=lng,
        is_default=default,
        created_at=at,
        updated_at=at,
    )
    return address


# --------------------------------------------------------------------------------------------
# Reload from the database
# --------------------------------------------------------------------------------------------


def load_world(conn: Any, cfg: GeneratorSettings) -> World:
    """Rebuild the world from the ``commerce`` schema (``conn`` is a psycopg connection)."""
    world = World(seed=cfg.seed)

    def rows(query: str) -> list[tuple]:
        return conn.execute(query).fetchall()

    for city_id, name, state, lat, lng in rows(
        "SELECT city_id, name, state, center_lat, center_lng FROM commerce.cities ORDER BY city_id"
    ):
        world.cities[city_id] = City(city_id, name, state, lat, lng)
    for r in rows(
        "SELECT store_id, city_id, name, lat, lng, service_radius_km, max_concurrent_orders, status, opened_at "
        "FROM commerce.dark_stores ORDER BY store_id"
    ):
        world.stores[r[0]] = Store(*r, demand_factor=_store_demand_factor(cfg.seed, r[0]))
    specs = {spec.name: spec for spec in CATEGORIES}
    for category_id, name in rows("SELECT category_id, name FROM commerce.categories ORDER BY category_id"):
        world.categories[category_id] = Category(category_id, specs[name])
    for r in rows(
        "SELECT product_id, sku, name, brand, category_id, unit, mrp, selling_price, is_active "
        "FROM commerce.products ORDER BY product_id"
    ):
        world.products[r[0]] = Product(*r, popularity=_product_popularity(cfg.seed, r[0]))
    for customer_id, city_id, signup_at in rows(
        "SELECT customer_id, city_id, signup_at FROM commerce.customers ORDER BY customer_id"
    ):
        propensity, churn_at = _customer_traits(cfg.seed, customer_id, signup_at)
        world.customers[customer_id] = Customer(customer_id, city_id, signup_at, propensity, churn_at)
    for r in rows(
        "SELECT address_id, customer_id, store_id, label, lat, lng, is_default "
        "FROM commerce.customer_addresses ORDER BY address_id"
    ):
        address = Address(*r)
        world.addresses[address.address_id] = address
        customer = world.customers[address.customer_id]
        customer.address_ids.append(address.address_id)
        if address.is_default:
            customer.default_address_id = address.address_id
    for store_id, product_id, on_hand, reorder_point, max_stock in rows(
        "SELECT store_id, product_id, on_hand, reorder_point, max_stock FROM commerce.inventory "
        "ORDER BY store_id, product_id"
    ):
        world.inventory[(store_id, product_id)] = StockLevel(on_hand, reorder_point, max_stock)
    for rider_id, store_id, status in rows(
        "SELECT rider_id, store_id, status FROM commerce.riders ORDER BY rider_id"
    ):
        vehicle, speed, shift_start, shift_hours = _rider_traits(cfg.seed, rider_id)
        world.riders[rider_id] = Rider(rider_id, store_id, vehicle, speed, shift_start, shift_hours, status)
    for r in rows(
        "SELECT promo_id, code, discount_type, discount_value, max_discount, min_order_value, starts_at, ends_at "
        "FROM commerce.promotions ORDER BY promo_id"
    ):
        world.promotions[r[0]] = Promotion(*r)

    sequences = {
        "customers": "SELECT max(customer_id) FROM commerce.customers",
        "addresses": "SELECT max(address_id) FROM commerce.customer_addresses",
        "riders": "SELECT max(rider_id) FROM commerce.riders",
        "promotions": "SELECT max(promo_id) FROM commerce.promotions",
        "orders": "SELECT max(order_id) FROM commerce.orders",
        "order_items": "SELECT max(order_item_id) FROM commerce.order_items",
        "status_events": "SELECT max(status_event_id) FROM commerce.order_status_history",
        "assignments": "SELECT max(assignment_id) FROM commerce.delivery_assignments",
        "payments": "SELECT max(payment_id) FROM commerce.payments",
        "refunds": "SELECT max(refund_id) FROM commerce.refunds",
        "shifts": "SELECT max(shift_id) FROM commerce.rider_shifts",
    }
    world.ids = IdAllocator({name: (conn.execute(q).fetchone()[0] or 0) + 1 for name, q in sequences.items()})
    has_tip = conn.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_schema = 'commerce' AND table_name = 'orders' "
        "AND column_name = 'tip_amount'"
    ).fetchone()
    world.schema_version = 2 if has_tip else 1
    world.rebuild_indexes()
    return world
