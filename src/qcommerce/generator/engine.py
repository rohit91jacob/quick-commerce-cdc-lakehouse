"""Discrete-event simulation of a quick-commerce operation.

Time only moves through a priority queue of events, every random draw comes from one seeded
``numpy`` generator, and ids are allocated by the application, so a given seed and start time always
produce the same stream of transactions (see ``tests/unit/test_generator_engine.py``).

Order lifecycle::

    placed -> accepted -> picking -> packed -> out_for_delivery -> delivered
       \\________\\___________\\___________________________-> cancelled

Every state change is one transaction touching several tables (orders, order_items, inventory,
payments, refunds, riders, delivery_assignments, order_status_history), which is what makes the
change stream realistic for CDC: multi-table transactions, hot rows (inventory, riders), deletes
(order items removed at picking, expired promotions, removed addresses, delisted SKUs) and a
mid-run schema change.
"""

from __future__ import annotations

import heapq
import itertools
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import numpy as np

from qcommerce.generator import demand
from qcommerce.generator.ops import Ddl, Txn
from qcommerce.generator.reference import PAYMENT_METHODS, ROLLING_FLASH_PROMO_PREFIX
from qcommerce.generator.world import (
    Promotion,
    Rider,
    StockLevel,
    Store,
    World,
    add_address,
    haversine_km,
    money,
    new_customer,
)
from qcommerce.settings import GeneratorSettings

ZERO = Decimal("0.00")
TERMINAL = ("delivered", "cancelled")
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "placed": {"accepted", "cancelled"},
    "accepted": {"picking", "cancelled"},
    "picking": {"packed", "cancelled"},
    "packed": {"out_for_delivery"},
    "out_for_delivery": {"delivered"},
}


@dataclass
class Item:
    order_item_id: int
    product_id: int
    quantity: int
    unit_price: Decimal
    mrp: Decimal
    substituted_for: int | None = None

    @property
    def line_total(self) -> Decimal:
        return money(self.unit_price * self.quantity)


@dataclass
class Order:
    order_id: int
    store_id: int
    customer_id: int
    placed_at: datetime
    distance_km: Decimal
    payment_id: int
    payment_method: str
    promised_minutes: int
    promo: Promotion | None
    items: dict[int, Item] = field(default_factory=dict)
    status: str = "placed"
    payment_status: str = "initiated"
    subtotal: Decimal = ZERO
    discount: Decimal = ZERO
    delivery_fee: Decimal = ZERO
    total: Decimal = ZERO
    refunded: Decimal = ZERO
    rider_id: int | None = None
    assignment_id: int | None = None

    @property
    def prepaid(self) -> bool:
        return self.payment_method != "cod"


class Simulation:
    def __init__(
        self,
        world: World,
        cfg: GeneratorSettings,
        start: datetime,
        *,
        rate_multiplier: float = 1.0,
        rng_stream: int = 2,
    ) -> None:
        self.world = world
        self.cfg = cfg
        self.rng = np.random.default_rng([cfg.seed, rng_stream])
        self.clock = start
        self.rate_multiplier = rate_multiplier
        self.orders: dict[int, Order] = {}
        self.pending_refunds: set[int] = set()
        self.stats: Counter[str] = Counter()
        self._queue: list[tuple[datetime, int, str, tuple[Any, ...]]] = []
        self._seq = itertools.count()
        self._delisted: set[tuple[int, int]] = set()
        self._started = False

    # ------------------------------------------------------------------ scheduling

    def schedule(self, at: datetime, kind: str, *args: Any) -> None:
        heapq.heappush(self._queue, (at, next(self._seq), kind, args))

    def start(self) -> None:
        """Schedule the recurring events. Call once before the first ``advance``."""
        if self._started:
            return
        self._started = True
        self.schedule(self.clock, "tick")
        for store in self.world.stores.values():
            self.schedule(self._next_ist(self.clock, 5, 30), "restock", store.store_id)
            self.schedule(self._next_ist(self.clock, 14, 30), "restock", store.store_id)
        for rider in self.world.riders.values():
            self.schedule(self._first_shift_start(rider), "shift_start", rider.rider_id)
        self.schedule(self.clock + timedelta(seconds=1), "daily_ops")

    def advance(self, until: datetime) -> Iterator[Txn]:
        """Process every event due at or before ``until``; yield the resulting transactions."""
        self.start()
        while self._queue and self._queue[0][0] <= until:
            at, _, kind, args = heapq.heappop(self._queue)
            self.clock = max(self.clock, at)
            txn = getattr(self, f"_on_{kind}")(*args)
            if txn is not None and txn.ops:
                self.stats[f"txn:{txn.label}"] += 1
                yield txn
        self.clock = max(self.clock, until)

    def schema_change(self) -> Txn:
        """Roll out migration V002 (``orders.tip_amount``); tips start flowing immediately after."""
        txn = Txn(self.clock, "ddl:orders_tip_amount")
        txn.ops.append(Ddl(2))
        self.world.schema_version = 2
        return txn

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _next_ist(after: datetime, hour: int, minute: int) -> datetime:
        local = demand.local(after)
        candidate = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if candidate <= local:
            candidate += timedelta(days=1)
        return candidate.astimezone(after.tzinfo)

    def _first_shift_start(self, rider: Rider) -> datetime:
        """Riders on shift right now log in immediately, others at their next shift start."""
        local = demand.local(self.clock)
        start_today = local.replace(hour=rider.shift_start_hour, minute=0, second=0, microsecond=0)
        for start in (start_today - timedelta(days=1), start_today):
            if start <= local < start + timedelta(hours=rider.shift_hours):
                return self.clock + timedelta(seconds=float(self.rng.uniform(1, 120)))
        return self._next_ist(self.clock, rider.shift_start_hour, 0) + timedelta(
            seconds=float(self.rng.uniform(0, 600))
        )

    def _after(self, low_s: float, high_s: float) -> datetime:
        return self.clock + timedelta(seconds=float(self.rng.uniform(low_s, high_s)))

    def _raining(self, city_id: int, at: datetime | None = None) -> bool:
        at = at or self.clock
        return any(start <= at < end for start, end in self.world.cities[city_id].rain_windows)

    def _history(self, txn: Txn, order: Order, status: str, actor: str) -> None:
        txn.insert(
            "order_status_history",
            status_event_id=self.world.ids.next("status_events"),
            order_id=order.order_id,
            status=status,
            actor=actor,
            occurred_at=self.clock,
            created_at=self.clock,
            updated_at=self.clock,
        )

    def _set_status(self, txn: Txn, order: Order, status: str, actor: str, **columns: Any) -> None:
        if status not in ALLOWED_TRANSITIONS.get(order.status, set()):
            raise RuntimeError(f"illegal transition {order.status} -> {status} for order {order.order_id}")
        order.status = status
        txn.update("orders", {"order_id": order.order_id}, status=status, updated_at=self.clock, **columns)
        self._history(txn, order, status, actor)

    def _adjust_stock(
        self, txn: Txn, store_id: int, product_id: int, delta: int | None = None, set_to: int | None = None
    ) -> None:
        level = self.world.inventory.get((store_id, product_id))
        if level is None:  # delisted while the order was in flight
            return
        level.on_hand = set_to if set_to is not None else max(0, level.on_hand + (delta or 0))
        txn.update(
            "inventory",
            {"store_id": store_id, "product_id": product_id},
            on_hand=level.on_hand,
            updated_at=self.clock,
        )

    def _price(self, order: Order) -> None:
        """Recompute subtotal, promo discount, delivery fee and total from the current items."""
        order.subtotal = money(sum((i.line_total for i in order.items.values()), ZERO))
        discount = ZERO
        promo = order.promo
        if promo is not None and order.subtotal >= promo.min_order_value:
            raw = (
                promo.discount_value
                if promo.discount_type == "flat"
                else order.subtotal * promo.discount_value / 100
            )
            discount = money(min(raw, promo.max_discount, order.subtotal))
        order.discount = discount
        store = self.world.stores[order.store_id]
        fee = Decimal("0") if order.subtotal >= Decimal("199") else Decimal("25")
        if self._raining(store.city_id, order.placed_at):
            fee += Decimal("15")
        if demand.local(order.placed_at).hour < 5:
            fee += Decimal("10")
        order.delivery_fee = money(fee)
        order.total = money(order.subtotal - order.discount + order.delivery_fee)

    def _refund(self, txn: Txn, order: Order, amount: Decimal, reason: str) -> None:
        amount = money(min(amount, order.total - order.refunded))
        if amount <= ZERO:
            return
        refund_id = self.world.ids.next("refunds")
        order.refunded += amount
        txn.insert(
            "refunds",
            refund_id=refund_id,
            order_id=order.order_id,
            payment_id=order.payment_id,
            amount=amount,
            reason=reason,
            status="initiated",
            created_at=self.clock,
            updated_at=self.clock,
        )
        order.payment_status = "refunded" if order.refunded >= order.total else "partially_refunded"
        txn.update(
            "payments", {"payment_id": order.payment_id}, status=order.payment_status, updated_at=self.clock
        )
        self.pending_refunds.add(refund_id)
        self.schedule(
            self.clock + timedelta(hours=float(self.rng.uniform(0.5, 6.0))), "refund_processed", refund_id
        )

    def _cancel(self, txn: Txn, order: Order, reason: str, actor: str) -> None:
        was_picking = order.status == "picking"
        self._set_status(txn, order, "cancelled", actor, cancelled_at=self.clock, cancel_reason=reason)
        for item in order.items.values():
            self._adjust_stock(txn, order.store_id, item.product_id, delta=item.quantity)
        if order.payment_status == "captured":
            self._refund(
                txn, order, order.total, "order_cancelled" if actor == "customer" else "payment_reversal"
            )
        elif order.payment_status == "initiated":
            order.payment_status = "failed"
            txn.update("payments", {"payment_id": order.payment_id}, status="failed", updated_at=self.clock)
        self.stats[f"cancelled:{reason}"] += 1
        self.orders.pop(order.order_id, None)
        if was_picking:
            store = self.world.stores[order.store_id]
            store.active_picking -= 1
            self._start_picking(txn, store)

    # ------------------------------------------------------------------ events: demand

    def _on_tick(self) -> None:
        for store in self.world.stores.values():
            if store.status != "active":
                continue
            rate = demand.order_rate_per_minute(
                self.cfg.base_orders_per_store_hour,
                self.clock,
                store.demand_factor,
                self._raining(store.city_id),
            )
            for _ in range(int(self.rng.poisson(rate * self.rate_multiplier))):
                self.schedule(self._after(0, 60), "order_arrival", store.store_id)
        self.schedule(self.clock + timedelta(minutes=1), "tick")

    def _pick_customer(self, store: Store, txn: Txn) -> tuple[int, int]:
        """Return (customer_id, address_id); sometimes a brand-new customer signs up."""
        active = [c for c in store.customer_pool if self.world.customers[c].churn_at > self.clock]
        if not active or self.rng.random() < 0.07:
            customer = new_customer(self.world, self.rng, txn, store.city_id, self.clock, store=store)
            store.customer_pool.append(customer.customer_id)
            self.stats["new_customers"] += 1
            return customer.customer_id, customer.default_address_id or 0
        weights = np.array([self.world.customers[c].propensity for c in active])
        customer = self.world.customers[active[int(self.rng.choice(len(active), p=weights / weights.sum()))]]
        options = [a for a in customer.address_ids if self.world.addresses[a].store_id == store.store_id]
        return customer.customer_id, (
            customer.default_address_id
            if customer.default_address_id in options
            else options[0]
            if options
            else customer.default_address_id or 0
        )

    def _choose_basket(self, store: Store) -> list[tuple[int, int, int | None]]:
        """Return [(product_id, quantity, substituted_for)] honouring stock; may be empty."""
        categories = list(self.world.categories.values())
        weights = np.array(
            [
                c.spec.demand_weight
                * demand.category_time_boost(self.clock, c.spec.morning_boost, c.spec.evening_boost)
                for c in categories
            ]
        )
        size = min(14, 1 + int(self.rng.poisson(3.0)))
        wanted: dict[int, int] = {}
        for _ in range(size):
            category = categories[int(self.rng.choice(len(categories), p=weights / weights.sum()))]
            pool = [
                p
                for p in self.world.products_by_category[category.category_id]
                if self.world.products[p].is_active
            ]
            pop = np.array([self.world.products[p].popularity for p in pool])
            product_id = pool[int(self.rng.choice(len(pool), p=pop / pop.sum()))]
            quantity = 1 if self.rng.random() < 0.72 else 2 if self.rng.random() < 0.8 else 3
            wanted[product_id] = wanted.get(product_id, 0) + quantity

        basket: dict[int, list[Any]] = {}  # product_id -> [quantity, substituted_for]

        def available(product_id: int) -> int:
            level = self.world.inventory.get((store.store_id, product_id))
            return (level.on_hand if level else 0) - (basket[product_id][0] if product_id in basket else 0)

        def add(product_id: int, quantity: int, substituted_for: int | None) -> None:
            if product_id in basket:
                basket[product_id][0] += quantity
            else:
                basket[product_id] = [quantity, substituted_for]

        for product_id, quantity in wanted.items():
            in_stock = available(product_id)
            if in_stock >= quantity:
                add(product_id, quantity, None)
            elif in_stock > 0:
                add(product_id, in_stock, None)
                self.stats["units_short"] += quantity - in_stock
            else:
                substitute = self._substitute(store, product_id, exclude=set(basket))
                if substitute is not None and self.rng.random() < 0.55:
                    add(substitute, min(quantity, available(substitute)), product_id)
                    self.stats["substitutions_at_checkout"] += 1
                else:
                    self.stats["units_lost_out_of_stock"] += quantity
        return [(p, q, s) for p, (q, s) in basket.items()]

    def _substitute(self, store: Store, product_id: int, exclude: set[int]) -> int | None:
        product = self.world.products[product_id]
        candidates = []
        for other in self.world.products_by_category[product.category_id]:
            if other == product_id or other in exclude:
                continue
            level = self.world.inventory.get((store.store_id, other))
            candidate = self.world.products[other]
            if (
                level
                and level.on_hand > 0
                and candidate.is_active
                and (
                    Decimal("0.7") * product.selling_price
                    <= candidate.selling_price
                    <= Decimal("1.3") * product.selling_price
                )
            ):
                candidates.append((candidate.popularity, other))
        return max(candidates)[1] if candidates else None

    def _active_promo(self) -> Promotion | None:
        live = [p for p in self.world.promotions.values() if p.starts_at <= self.clock < p.ends_at]
        if not live:
            return None
        usage = 0.35 if demand.is_festival(self.clock) else 0.16
        if self.rng.random() >= usage:
            return None
        flash = [p for p in live if p.code.startswith(ROLLING_FLASH_PROMO_PREFIX)]
        weekend = demand.local(self.clock).weekday() >= 5
        choices = flash or [p for p in live if weekend or p.code != "WEEKEND20"]
        return choices[int(self.rng.integers(len(choices)))] if choices else None

    def _on_order_arrival(self, store_id: int) -> Txn | None:
        store = self.world.stores[store_id]
        if store.status != "active":
            self.stats["sessions_lost_store_paused"] += 1
            return None
        basket = self._choose_basket(store)
        if not basket or all(q == 0 for _, q, _ in basket):
            self.stats["sessions_lost_empty_basket"] += 1
            return None
        txn = Txn(self.clock, "order_placed")
        customer_id, address_id = self._pick_customer(store, txn)
        address = self.world.addresses[address_id]
        distance = money(round(haversine_km(store.lat, store.lng, address.lat, address.lng) * 1.25, 2))
        names = [m for m, _ in PAYMENT_METHODS]
        shares = np.array([s for _, s in PAYMENT_METHODS])
        method = names[int(self.rng.choice(len(names), p=shares / shares.sum()))]
        promised = self.cfg.promised_minutes + (5 if self._raining(store.city_id) else 0)
        order = Order(
            self.world.ids.next("orders"),
            store_id,
            customer_id,
            self.clock,
            distance,
            self.world.ids.next("payments"),
            method,
            promised,
            self._active_promo(),
        )
        for product_id, quantity, substituted_for in basket:
            if quantity <= 0:
                continue
            product = self.world.products[product_id]
            item = Item(
                self.world.ids.next("order_items"),
                product_id,
                quantity,
                product.selling_price,
                product.mrp,
                substituted_for,
            )
            order.items[item.order_item_id] = item
        self._price(order)
        txn.insert(
            "orders",
            order_id=order.order_id,
            customer_id=customer_id,
            store_id=store_id,
            address_id=address_id,
            delivery_lat=address.lat,
            delivery_lng=address.lng,
            distance_km=distance,
            status="placed",
            promo_code=order.promo.code if order.promo else None,
            items_count=len(order.items),
            subtotal=order.subtotal,
            discount=order.discount,
            delivery_fee=order.delivery_fee,
            total=order.total,
            payment_method=method,
            promised_minutes=promised,
            placed_at=self.clock,
            created_at=self.clock,
            updated_at=self.clock,
        )
        for item in order.items.values():
            txn.insert(
                "order_items",
                order_item_id=item.order_item_id,
                order_id=order.order_id,
                product_id=item.product_id,
                quantity=item.quantity,
                unit_price=item.unit_price,
                mrp=item.mrp,
                line_total=item.line_total,
                substituted_for_product_id=item.substituted_for,
                created_at=self.clock,
                updated_at=self.clock,
            )
            self._adjust_stock(txn, store_id, item.product_id, delta=-item.quantity)
        self._history(txn, order, "placed", "customer")
        txn.insert(
            "payments",
            payment_id=order.payment_id,
            order_id=order.order_id,
            method=method,
            amount=order.total,
            status="initiated",
            provider_ref=f"PAY{order.payment_id:010d}",
            created_at=self.clock,
            updated_at=self.clock,
        )
        self.orders[order.order_id] = order
        self.stats["orders_placed"] += 1
        if order.prepaid:
            self.schedule(self._after(4, 25), "payment_result", order.order_id)
        else:
            self.schedule(self._after(10, 40), "accept", order.order_id)
        if self.rng.random() < 0.025:
            self.schedule(self._after(30, 300), "customer_cancel", order.order_id)
        return txn

    # ------------------------------------------------------------------ events: fulfilment

    def _on_payment_result(self, order_id: int) -> Txn | None:
        order = self.orders.get(order_id)
        if order is None or order.status != "placed":
            return None
        txn = Txn(self.clock, "payment_result")
        if self.rng.random() < 0.03:
            self._cancel(txn, order, "payment_failed", "system")
            return txn
        order.payment_status = "captured"
        txn.update("payments", {"payment_id": order.payment_id}, status="captured", updated_at=self.clock)
        self.schedule(self._after(8, 35), "accept", order_id)
        return txn

    def _on_accept(self, order_id: int) -> Txn | None:
        order = self.orders.get(order_id)
        if order is None or order.status != "placed":
            return None
        txn = Txn(self.clock, "order_accepted")
        self._set_status(txn, order, "accepted", "store", accepted_at=self.clock)
        store = self.world.stores[order.store_id]
        store.pick_queue.append(order_id)
        self._start_picking(txn, store)
        return txn

    def _start_picking(self, txn: Txn, store: Store) -> None:
        while store.pick_queue and store.active_picking < store.max_concurrent_orders:
            order = self.orders.get(store.pick_queue.popleft())
            if order is None or order.status != "accepted":
                continue
            store.active_picking += 1
            self._set_status(txn, order, "picking", "store", picking_started_at=self.clock)
            units = sum(i.quantity for i in order.items.values())
            seconds = (25 + 12 * units) * float(self.rng.lognormal(0.0, 0.25)) + 10
            self.schedule(self.clock + timedelta(seconds=seconds), "pick_complete", order.order_id)

    def _on_pick_complete(self, order_id: int) -> Txn | None:
        order = self.orders.get(order_id)
        if order is None or order.status != "picking":
            return None
        txn = Txn(self.clock, "order_packed")
        store = self.world.stores[order.store_id]
        before = order.total
        removals: list[Item] = []
        for item in list(order.items.values()):
            if self.rng.random() >= 0.015:
                continue
            # The shelf is empty although the system said otherwise: correct stock to zero.
            self._adjust_stock(txn, store.store_id, item.product_id, set_to=0)
            substitute = self._substitute(
                store, item.product_id, exclude={i.product_id for i in order.items.values()}
            )
            if substitute is not None and self.rng.random() < 0.5:
                product = self.world.products[substitute]
                sub_level = self.world.inventory[(store.store_id, substitute)]
                quantity = min(item.quantity, sub_level.on_hand)
                item.substituted_for, item.product_id = item.product_id, substitute
                item.quantity, item.unit_price, item.mrp = quantity, product.selling_price, product.mrp
                txn.update(
                    "order_items",
                    {"order_item_id": item.order_item_id},
                    product_id=substitute,
                    quantity=quantity,
                    unit_price=item.unit_price,
                    mrp=item.mrp,
                    line_total=item.line_total,
                    substituted_for_product_id=item.substituted_for,
                    updated_at=self.clock,
                )
                self._adjust_stock(txn, store.store_id, substitute, delta=-quantity)
                self.stats["substitutions_at_picking"] += 1
            else:
                removals.append(item)
        if removals and len(removals) == len(order.items):
            # Nothing left to deliver: cancel the order and keep its lines for the record.
            store.active_picking -= 1
            self._cancel_after_picking(txn, order, store)
            return txn
        for item in removals:
            del order.items[item.order_item_id]
            txn.delete("order_items", order_item_id=item.order_item_id)
            self.stats["items_removed_at_picking"] += 1
        self._price(order)
        if order.total != before:
            txn.update(
                "orders",
                {"order_id": order_id},
                items_count=len(order.items),
                subtotal=order.subtotal,
                discount=order.discount,
                delivery_fee=order.delivery_fee,
                total=order.total,
                updated_at=self.clock,
            )
            if order.payment_status in ("captured", "partially_refunded") and before > order.total:
                self._refund(txn, order, before - order.total, "items_unavailable")
            elif order.payment_status == "initiated" and not order.prepaid:
                txn.update(
                    "payments", {"payment_id": order.payment_id}, amount=order.total, updated_at=self.clock
                )
        self._set_status(txn, order, "packed", "store", packed_at=self.clock)
        store.active_picking -= 1
        self._start_picking(txn, store)
        store.dispatch_queue.append(order_id)
        self._dispatch(txn, store)
        return txn

    def _cancel_after_picking(self, txn: Txn, order: Order, store: Store) -> None:
        """Every item turned out to be unavailable."""
        self._set_status(
            txn, order, "cancelled", "store", cancelled_at=self.clock, cancel_reason="items_unavailable"
        )
        if order.payment_status == "captured":
            self._refund(txn, order, order.total, "items_unavailable")
        else:
            order.payment_status = "failed"
            txn.update("payments", {"payment_id": order.payment_id}, status="failed", updated_at=self.clock)
        self.stats["cancelled:items_unavailable"] += 1
        self.orders.pop(order.order_id, None)
        self._start_picking(txn, store)

    def _dispatch(self, txn: Txn, store: Store) -> None:
        while store.dispatch_queue and store.idle_riders:
            order = self.orders.get(store.dispatch_queue.popleft())
            if order is None or order.status != "packed":
                continue
            rider = self.world.riders[store.idle_riders.popleft()]
            rider.status = "assigned"
            order.rider_id, order.assignment_id = rider.rider_id, self.world.ids.next("assignments")
            txn.insert(
                "delivery_assignments",
                assignment_id=order.assignment_id,
                order_id=order.order_id,
                rider_id=rider.rider_id,
                assigned_at=self.clock,
                distance_km=order.distance_km,
                created_at=self.clock,
                updated_at=self.clock,
            )
            txn.update("riders", {"rider_id": rider.rider_id}, status="assigned", updated_at=self.clock)
            self.schedule(self._after(15, 50), "rider_pickup", order.order_id)

    def _travel_minutes(self, rider: Rider, distance_km: Decimal, city_id: int) -> float:
        minutes = float(distance_km) / rider.speed_kmh * 60.0
        minutes *= demand.traffic_factor(self.clock) * (1.4 if self._raining(city_id) else 1.0)
        return minutes * float(self.rng.lognormal(0.0, 0.18))

    def _on_rider_pickup(self, order_id: int) -> Txn | None:
        order = self.orders.get(order_id)
        if order is None or order.status != "packed" or order.rider_id is None:
            return None
        rider = self.world.riders[order.rider_id]
        store = self.world.stores[order.store_id]
        txn = Txn(self.clock, "order_dispatched")
        rider.status = "delivering"
        txn.update(
            "delivery_assignments",
            {"assignment_id": order.assignment_id},
            picked_up_at=self.clock,
            updated_at=self.clock,
        )
        txn.update("riders", {"rider_id": rider.rider_id}, status="delivering", updated_at=self.clock)
        self._set_status(txn, order, "out_for_delivery", "rider", dispatched_at=self.clock)
        minutes = self._travel_minutes(rider, order.distance_km, store.city_id) + float(
            self.rng.uniform(0.5, 1.5)
        )
        self.schedule(self.clock + timedelta(minutes=minutes), "delivered", order_id)
        return txn

    def _on_delivered(self, order_id: int) -> Txn | None:
        order = self.orders.get(order_id)
        if order is None or order.status != "out_for_delivery" or order.rider_id is None:
            return None
        rider = self.world.riders[order.rider_id]
        store = self.world.stores[order.store_id]
        txn = Txn(self.clock, "order_delivered")
        extra: dict[str, Any] = {}
        if self.world.schema_version >= 2 and self.rng.random() < 0.22:
            extra["tip_amount"] = money(int(self.rng.choice([10, 20, 20, 30, 50])))
            self.stats["tips"] += 1
        self._set_status(txn, order, "delivered", "rider", delivered_at=self.clock, **extra)
        txn.update(
            "delivery_assignments",
            {"assignment_id": order.assignment_id},
            delivered_at=self.clock,
            updated_at=self.clock,
        )
        if not order.prepaid:
            order.payment_status = "captured"
            txn.update(
                "payments",
                {"payment_id": order.payment_id},
                status="captured",
                amount=order.total,
                updated_at=self.clock,
            )
        elapsed = (self.clock - order.placed_at).total_seconds() / 60.0
        if order.prepaid and elapsed > order.promised_minutes + 8 and self.rng.random() < 0.3:
            self._refund(txn, order, min(Decimal("50"), money(order.total / 10)), "late_delivery")
        rider.status = "returning"
        txn.update("riders", {"rider_id": rider.rider_id}, status="returning", updated_at=self.clock)
        back = self._travel_minutes(rider, order.distance_km, store.city_id) * 0.9
        self.schedule(self.clock + timedelta(minutes=back), "rider_returned", rider.rider_id)
        self.stats["orders_delivered"] += 1
        self.stats["sla_breached" if elapsed > order.promised_minutes else "sla_met"] += 1
        self.orders.pop(order_id, None)
        return txn

    def _on_rider_returned(self, rider_id: int) -> Txn | None:
        rider = self.world.riders[rider_id]
        if rider.status != "returning":
            return None
        txn = Txn(self.clock, "rider_returned")
        if rider.ending:
            self._end_shift(txn, rider)
            return txn
        rider.status = "idle"
        txn.update("riders", {"rider_id": rider_id}, status="idle", updated_at=self.clock)
        store = self.world.stores[rider.store_id]
        store.idle_riders.append(rider_id)
        self._dispatch(txn, store)
        return txn

    def _on_customer_cancel(self, order_id: int) -> Txn | None:
        order = self.orders.get(order_id)
        if order is None or order.status not in ("placed", "accepted", "picking"):
            return None
        txn = Txn(self.clock, "order_cancelled")
        self._cancel(txn, order, "customer_cancelled", "customer")
        return txn

    def _on_refund_processed(self, refund_id: int) -> Txn | None:
        if refund_id not in self.pending_refunds:
            return None
        self.pending_refunds.discard(refund_id)
        txn = Txn(self.clock, "refund_processed")
        txn.update("refunds", {"refund_id": refund_id}, status="processed", updated_at=self.clock)
        return txn

    # ------------------------------------------------------------------ events: workforce

    def _on_shift_start(self, rider_id: int) -> Txn | None:
        rider = self.world.riders[rider_id]
        if rider.status != "offline":  # still finishing yesterday's last delivery; try again tomorrow
            self.schedule(self._next_ist(self.clock, rider.shift_start_hour, 0), "shift_start", rider_id)
            return None
        txn = Txn(self.clock, "shift_started")
        rider.shift_id, rider.status, rider.ending = self.world.ids.next("shifts"), "idle", False
        txn.insert(
            "rider_shifts",
            shift_id=rider.shift_id,
            rider_id=rider_id,
            store_id=rider.store_id,
            started_at=self.clock,
            created_at=self.clock,
            updated_at=self.clock,
        )
        txn.update("riders", {"rider_id": rider_id}, status="idle", updated_at=self.clock)
        store = self.world.stores[rider.store_id]
        store.idle_riders.append(rider_id)
        self._dispatch(txn, store)
        self.schedule(self.clock + timedelta(hours=rider.shift_hours), "shift_end", rider_id)
        self.schedule(
            self._next_ist(self.clock + timedelta(hours=rider.shift_hours), rider.shift_start_hour, 0)
            + timedelta(seconds=float(self.rng.uniform(0, 600))),
            "shift_start",
            rider_id,
        )
        return txn

    def _on_shift_end(self, rider_id: int) -> Txn | None:
        rider = self.world.riders[rider_id]
        if rider.status == "offline":
            return None
        if rider.status != "idle":
            rider.ending = True  # finish the current delivery first
            return None
        txn = Txn(self.clock, "shift_ended")
        self.world.stores[rider.store_id].idle_riders.remove(rider_id)
        self._end_shift(txn, rider)
        return txn

    def _end_shift(self, txn: Txn, rider: Rider) -> None:
        rider.status, rider.ending = "offline", False
        if rider.shift_id is not None:
            txn.update(
                "rider_shifts", {"shift_id": rider.shift_id}, ended_at=self.clock, updated_at=self.clock
            )
            rider.shift_id = None
        txn.update("riders", {"rider_id": rider.rider_id}, status="offline", updated_at=self.clock)

    # ------------------------------------------------------------------ events: back office

    def _on_restock(self, store_id: int) -> Txn | None:
        txn = Txn(self.clock, "restock")
        for (sid, product_id), level in self.world.inventory.items():
            if sid == store_id and level.on_hand <= level.reorder_point:
                level.on_hand = level.max_stock
                txn.update(
                    "inventory",
                    {"store_id": store_id, "product_id": product_id},
                    on_hand=level.on_hand,
                    last_restocked_at=self.clock,
                    updated_at=self.clock,
                )
        self.schedule(self.clock + timedelta(days=1), "restock", store_id)
        return txn

    def _on_daily_ops(self) -> Txn:
        txn = Txn(self.clock, "daily_ops")
        world, rng = self.world, self.rng
        # Pricing team: selling-price moves on a few SKUs, rare MRP revisions (SCD2 history downstream).
        for product in world.products.values():
            roll = rng.random()
            if roll < 0.003:
                product.mrp = money(int(float(product.mrp) * float(rng.uniform(1.05, 1.12))) + 1)
                product.selling_price = min(product.selling_price, product.mrp)
                txn.update(
                    "products",
                    {"product_id": product.product_id},
                    mrp=product.mrp,
                    selling_price=product.selling_price,
                    updated_at=self.clock,
                )
            elif roll < 0.028:
                product.selling_price = money(max(1, int(float(product.mrp) * float(rng.uniform(0.72, 1.0)))))
                txn.update(
                    "products",
                    {"product_id": product.product_id},
                    selling_price=product.selling_price,
                    updated_at=self.clock,
                )
        # Store operations: catchment and picker-capacity changes, occasional pauses (SCD2 history).
        for store in world.stores.values():
            if rng.random() < 0.12:
                delta = Decimal(str(rng.choice([-0.5, -0.25, 0.25, 0.5])))
                store.service_radius_km = money(
                    min(Decimal("4.0"), max(Decimal("1.5"), store.service_radius_km + delta))
                )
                txn.update(
                    "dark_stores",
                    {"store_id": store.store_id},
                    service_radius_km=store.service_radius_km,
                    updated_at=self.clock,
                )
            if rng.random() < 0.08:
                store.max_concurrent_orders = int(
                    min(10, max(3, store.max_concurrent_orders + rng.choice([-1, 1])))
                )
                txn.update(
                    "dark_stores",
                    {"store_id": store.store_id},
                    max_concurrent_orders=store.max_concurrent_orders,
                    updated_at=self.clock,
                )
            if rng.random() < 0.06:
                pause_at = self._next_ist(self.clock, int(rng.integers(13, 21)), int(rng.integers(0, 60)))
                self.schedule(pause_at, "store_pause", store.store_id)
                self.schedule(
                    pause_at + timedelta(minutes=float(rng.uniform(40, 110))), "store_resume", store.store_id
                )
        # Weather: evening showers raise demand and slow riders down.
        for city in world.cities.values():
            city.rain_windows = [w for w in city.rain_windows if w[1] > self.clock]
            if rng.random() < 0.18:
                start = self._next_ist(self.clock, int(rng.integers(15, 21)), 0)
                city.rain_windows.append((start, start + timedelta(hours=float(rng.uniform(1.5, 3.5)))))
        # Marketing: a daily flash promo; expired flash promos are hard-deleted two days later.
        day = demand.local(self.clock)
        code = f"{ROLLING_FLASH_PROMO_PREFIX}{day:%Y%m%d}"
        if all(p.code != code for p in world.promotions.values()):
            starts = self._next_ist(self.clock, 10, 0)
            promo = Promotion(
                world.ids.next("promotions"),
                code,
                "percent",
                Decimal("25.00"),
                Decimal("100.00"),
                Decimal("249.00"),
                starts,
                starts + timedelta(hours=13, minutes=59),
            )
            world.promotions[promo.promo_id] = promo
            txn.insert(
                "promotions",
                promo_id=promo.promo_id,
                code=code,
                description="25% off up to 100 today only",
                discount_type="percent",
                discount_value=promo.discount_value,
                max_discount=promo.max_discount,
                min_order_value=promo.min_order_value,
                starts_at=promo.starts_at,
                ends_at=promo.ends_at,
                created_at=self.clock,
                updated_at=self.clock,
            )
        for promo in list(world.promotions.values()):
            if promo.code.startswith(ROLLING_FLASH_PROMO_PREFIX) and promo.ends_at < self.clock - timedelta(
                days=2
            ):
                del world.promotions[promo.promo_id]
                txn.delete("promotions", promo_id=promo.promo_id)
        # Customers add new addresses and remove old ones.
        for customer in list(world.customers.values()):
            roll = rng.random()
            if roll < 0.012:
                stores = world.stores_in_city(customer.city_id)
                add_address(
                    world,
                    rng,
                    txn,
                    customer,
                    stores[int(rng.integers(len(stores)))],
                    self.clock,
                    label="work" if rng.random() < 0.6 else "other",
                    default=False,
                )
            elif roll < 0.024 and len(customer.address_ids) > 1:
                removable = [a for a in customer.address_ids if a != customer.default_address_id]
                if removable:
                    address_id = removable[int(rng.integers(len(removable)))]
                    customer.address_ids.remove(address_id)
                    del world.addresses[address_id]
                    txn.delete("customer_addresses", address_id=address_id)
        # Category management: delist a few SKUs per store (empty shelves first) and relist them later.
        for key, level in list(world.inventory.items()):
            if rng.random() < (0.05 if level.on_hand == 0 else 0.006):
                del world.inventory[key]
                self._delisted.add(key)
                txn.delete("inventory", store_id=key[0], product_id=key[1])
                self.schedule(self.clock + timedelta(days=float(rng.uniform(1, 4))), "relist", key[0], key[1])
        self.schedule(self._next_ist(self.clock, 6, 0), "daily_ops")
        return txn

    def _on_store_pause(self, store_id: int) -> Txn | None:
        store = self.world.stores[store_id]
        if store.status != "active":
            return None
        store.status = "paused"
        txn = Txn(self.clock, "store_paused")
        txn.update("dark_stores", {"store_id": store_id}, status="paused", updated_at=self.clock)
        return txn

    def _on_store_resume(self, store_id: int) -> Txn | None:
        store = self.world.stores[store_id]
        if store.status != "paused":
            return None
        store.status = "active"
        txn = Txn(self.clock, "store_resumed")
        txn.update("dark_stores", {"store_id": store_id}, status="active", updated_at=self.clock)
        return txn

    def _on_relist(self, store_id: int, product_id: int) -> Txn | None:
        key = (store_id, product_id)
        if key not in self._delisted or key in self.world.inventory:
            return None
        self._delisted.discard(key)
        max_stock = int(self.rng.integers(20, 60))
        self.world.inventory[key] = StockLevel(max_stock, max(2, max_stock // 4), max_stock)
        txn = Txn(self.clock, "relisted")
        # Same composite key as the deleted row: downstream must resurrect it correctly.
        txn.insert(
            "inventory",
            store_id=store_id,
            product_id=product_id,
            on_hand=max_stock,
            reorder_point=max(2, max_stock // 4),
            max_stock=max_stock,
            last_restocked_at=self.clock,
            created_at=self.clock,
            updated_at=self.clock,
        )
        return txn

    # ------------------------------------------------------------------ recovery

    def recover(
        self, inflight: list[dict[str, Any]], open_shifts: list[tuple[int, int]], busy_riders: list[int]
    ) -> Txn:
        """Close out work left in flight by a previous generator process (crash/restart)."""
        txn = Txn(self.clock, "recovery")
        for row in inflight:
            order_id = row["order_id"]
            txn.update(
                "orders",
                {"order_id": order_id},
                status="cancelled",
                cancelled_at=self.clock,
                cancel_reason="system_recovery",
                updated_at=self.clock,
            )
            txn.insert(
                "order_status_history",
                status_event_id=self.world.ids.next("status_events"),
                order_id=order_id,
                status="cancelled",
                actor="system",
                occurred_at=self.clock,
                created_at=self.clock,
                updated_at=self.clock,
            )
            for product_id, quantity in row["items"]:
                self._adjust_stock(txn, row["store_id"], product_id, delta=quantity)
            if row["payment_status"] in ("captured", "partially_refunded"):
                amount = money(row["total"] - row["refunded"])
                if amount > ZERO:
                    refund_id = self.world.ids.next("refunds")
                    txn.insert(
                        "refunds",
                        refund_id=refund_id,
                        order_id=order_id,
                        payment_id=row["payment_id"],
                        amount=amount,
                        reason="payment_reversal",
                        status="initiated",
                        created_at=self.clock,
                        updated_at=self.clock,
                    )
                    self.pending_refunds.add(refund_id)
                    self.schedule(self.clock + timedelta(hours=1), "refund_processed", refund_id)
                txn.update(
                    "payments", {"payment_id": row["payment_id"]}, status="refunded", updated_at=self.clock
                )
            elif row["payment_status"] == "initiated":
                txn.update(
                    "payments", {"payment_id": row["payment_id"]}, status="failed", updated_at=self.clock
                )
        for shift_id, _rider_id in open_shifts:
            txn.update("rider_shifts", {"shift_id": shift_id}, ended_at=self.clock, updated_at=self.clock)
        for rider_id in busy_riders:
            txn.update("riders", {"rider_id": rider_id}, status="offline", updated_at=self.clock)
        for rider in self.world.riders.values():
            rider.status, rider.shift_id = "offline", None
        self.stats["recovered_orders"] += len(inflight)
        return txn
