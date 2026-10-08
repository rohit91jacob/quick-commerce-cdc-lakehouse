"""Demand curves: diurnal, weekly and festival multipliers on the base order rate."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from qcommerce.generator.reference import FESTIVALS

IST = timezone(timedelta(hours=5, minutes=30), name="IST")

# Relative order intake per IST hour-of-day (breakfast and dinner peaks; mean ~1.0).
_HOURLY = (
    0.30,
    0.18,
    0.10,
    0.06,
    0.06,
    0.12,
    0.40,
    0.85,
    1.30,
    1.45,
    1.15,
    1.00,
    1.05,
    1.10,
    0.95,
    0.90,
    1.00,
    1.25,
    1.60,
    1.90,
    2.05,
    1.80,
    1.20,
    0.65,
)
_WEEKDAY = (0.92, 0.90, 0.93, 0.95, 1.08, 1.25, 1.22)  # Monday .. Sunday


def local(ts: datetime) -> datetime:
    return ts.astimezone(IST)


def hourly_multiplier(ts: datetime) -> float:
    """Smoothly interpolated hour-of-day factor."""
    t = local(ts)
    hour = t.hour + t.minute / 60.0
    lo = int(hour) % 24
    hi = (lo + 1) % 24
    frac = hour - int(hour)
    return _HOURLY[lo] * (1 - frac) + _HOURLY[hi] * frac


def weekday_multiplier(ts: datetime) -> float:
    return _WEEKDAY[local(ts).weekday()]


def festival_multiplier(ts: datetime) -> float:
    return FESTIVALS.get(local(ts).strftime("%m-%d"), 1.0)


def is_festival(ts: datetime) -> bool:
    return local(ts).strftime("%m-%d") in FESTIVALS


def order_rate_per_minute(base_per_hour: float, ts: datetime, store_factor: float, rain: bool) -> float:
    """Expected orders per minute for one store at ``ts``."""
    rate = base_per_hour / 60.0
    rate *= hourly_multiplier(ts) * weekday_multiplier(ts) * festival_multiplier(ts) * store_factor
    if rain:
        rate *= 1.2  # nobody wants to go out in the rain
    return rate


def traffic_factor(ts: datetime) -> float:
    """Multiplier on travel time (>1 means slower) by IST hour."""
    hour = local(ts).hour
    if 8 <= hour <= 10 or 18 <= hour <= 21:
        return 1.30
    if 0 <= hour <= 5:
        return 0.85
    return 1.0


def category_time_boost(ts: datetime, morning_boost: float, evening_boost: float) -> float:
    hour = local(ts).hour
    if 6 <= hour < 11:
        return morning_boost
    if 18 <= hour < 23:
        return evening_boost
    return 1.0
