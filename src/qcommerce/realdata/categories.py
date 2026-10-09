"""Map Open Food Facts category tags onto the simulated store's catalogue categories.

The first matching rule wins, so specific rules (tea, coffee) come before broad ones (beverages).
Products that match nothing are left out of the store catalogue (they still appear in the
market-price tables).
"""

from __future__ import annotations

from collections.abc import Iterable

RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Tea, Coffee & Breakfast", ("en:teas", "en:tea", "en:coffees", "en:instant-coffees",
                                 "en:breakfast-cereals", "en:mueslis", "en:oat-flakes", "en:honeys")),
    ("Dairy, Bread & Eggs", ("en:milks", "en:dairies", "en:cheeses", "en:yogurts", "en:butters", "en:ghee",
                             "en:breads", "en:eggs", "en:paneer", "en:dairy-desserts")),
    ("Atta, Rice & Dal", ("en:flours", "en:wheat-flours", "en:rices", "en:legumes", "en:pulses",
                          "en:lentils", "en:chickpeas", "en:semolinas", "en:cereal-grains")),
    ("Instant & Frozen Food", ("en:instant-noodles", "en:noodles", "en:frozen-foods", "en:pastas",
                               "en:soups", "en:meals", "en:ready-meals", "en:pickles", "en:sauces")),
    ("Cold Drinks & Juices", ("en:sodas", "en:juices", "en:fruit-juices", "en:waters", "en:energy-drinks",
                              "en:carbonated-drinks", "en:plant-based-beverages", "en:beverages")),
    ("Snacks & Munchies", ("en:snacks", "en:salty-snacks", "en:sweet-snacks", "en:chips-and-fries",
                           "en:biscuits", "en:cookies", "en:chocolates", "en:confectioneries",
                           "en:nuts", "en:namkeen")),
)  # fmt: skip


def map_category(tags: Iterable[str] | None) -> str | None:
    found = set(tags or ())
    for category, needles in RULES:
        if found.intersection(needles):
            return category
    return None
