"""Static reference data for the simulated business. Every brand name is fictional."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CitySpec:
    name: str
    state: str
    lat: float
    lng: float


CITIES: tuple[CitySpec, ...] = (
    CitySpec("Bengaluru", "Karnataka", 12.9716, 77.5946),
    CitySpec("Mumbai", "Maharashtra", 19.0760, 72.8777),
    CitySpec("Gurugram", "Haryana", 28.4595, 77.0266),
    CitySpec("Hyderabad", "Telangana", 17.3850, 78.4867),
    CitySpec("Pune", "Maharashtra", 18.5204, 73.8567),
)

NEIGHBOURHOODS: dict[str, tuple[str, ...]] = {
    "Bengaluru": ("Indiranagar", "Koramangala", "HSR Layout", "Whitefield", "Jayanagar", "Hebbal"),
    "Mumbai": ("Andheri West", "Bandra", "Powai", "Lower Parel", "Chembur", "Borivali"),
    "Gurugram": ("DLF Phase 3", "Sohna Road", "Golf Course Road", "Sector 56", "Palam Vihar", "MG Road"),
    "Hyderabad": ("Gachibowli", "Madhapur", "Banjara Hills", "Kondapur", "Kukatpally", "Begumpet"),
    "Pune": ("Koregaon Park", "Baner", "Kothrud", "Viman Nagar", "Hinjewadi", "Aundh"),
}


@dataclass(frozen=True)
class CategorySpec:
    name: str
    perishable: bool
    demand_weight: float
    price_range: tuple[int, int]
    morning_boost: float  # demand multiplier 06:00-11:00 IST
    evening_boost: float  # demand multiplier 18:00-23:00 IST
    brands: tuple[str, ...]
    items: tuple[str, ...]
    units: tuple[str, ...]


CATEGORIES: tuple[CategorySpec, ...] = (
    CategorySpec(
        "Fruits & Vegetables",
        True,
        0.16,
        (18, 220),
        1.3,
        1.0,
        ("FreshNest", "GreenBasket", "FarmToFork"),
        (
            "Tomato",
            "Onion",
            "Potato",
            "Banana Robusta",
            "Apple Shimla",
            "Coriander",
            "Cucumber",
            "Carrot",
            "Lemon",
            "Spinach",
            "Capsicum",
            "Pomegranate",
        ),
        ("250 g", "500 g", "1 kg", "6 pcs", "1 bunch"),
    ),
    CategorySpec(
        "Dairy, Bread & Eggs",
        True,
        0.15,
        (22, 140),
        1.6,
        0.8,
        ("Dairy Dawn", "MooFresh", "Golden Crust"),
        (
            "Toned Milk",
            "Curd",
            "Paneer",
            "Butter",
            "Brown Bread",
            "White Bread",
            "Eggs",
            "Cheese Slices",
            "Buttermilk",
            "Ghee",
        ),
        ("200 g", "500 ml", "1 l", "6 pcs", "12 pcs", "400 g"),
    ),
    CategorySpec(
        "Snacks & Munchies",
        False,
        0.12,
        (20, 210),
        0.7,
        1.5,
        ("Crunchly", "MunchBox", "SpiceRoute"),
        (
            "Potato Chips",
            "Nachos",
            "Bhujia",
            "Roasted Peanuts",
            "Cookies",
            "Popcorn",
            "Khakhra",
            "Trail Mix",
            "Chocolate Bar",
            "Wafers",
        ),
        ("50 g", "90 g", "150 g", "200 g", "400 g"),
    ),
    CategorySpec(
        "Cold Drinks & Juices",
        False,
        0.09,
        (20, 190),
        0.6,
        1.4,
        ("FizzUp", "OrchardPress", "CoolSip"),
        (
            "Cola",
            "Lemon Soda",
            "Orange Juice",
            "Mango Drink",
            "Coconut Water",
            "Energy Drink",
            "Iced Tea",
            "Mineral Water",
        ),
        ("250 ml", "500 ml", "750 ml", "1 l", "2 l"),
    ),
    CategorySpec(
        "Atta, Rice & Dal",
        False,
        0.08,
        (55, 480),
        1.0,
        1.0,
        ("GoldGrain", "KisanMill", "RoyalHarvest"),
        (
            "Whole Wheat Atta",
            "Basmati Rice",
            "Sona Masoori Rice",
            "Toor Dal",
            "Moong Dal",
            "Chana Dal",
            "Poha",
            "Besan",
        ),
        ("500 g", "1 kg", "5 kg"),
    ),
    CategorySpec(
        "Instant & Frozen Food",
        False,
        0.07,
        (35, 360),
        0.8,
        1.3,
        ("QuickBite", "FrostBay", "NoodleNest"),
        (
            "Instant Noodles",
            "Frozen Peas",
            "Veg Momos",
            "French Fries",
            "Ready Biryani",
            "Frozen Paratha",
            "Pasta",
            "Soup Mix",
        ),
        ("70 g", "200 g", "400 g", "500 g", "1 kg"),
    ),
    CategorySpec(
        "Tea, Coffee & Breakfast",
        False,
        0.06,
        (45, 420),
        1.5,
        0.7,
        ("BrewMorning", "HillLeaf", "OatCo"),
        (
            "Assam Tea",
            "Green Tea",
            "Instant Coffee",
            "Filter Coffee",
            "Corn Flakes",
            "Rolled Oats",
            "Muesli",
            "Honey",
        ),
        ("100 g", "250 g", "500 g", "1 kg"),
    ),
    CategorySpec(
        "Cleaning Essentials",
        False,
        0.06,
        (40, 420),
        1.0,
        0.9,
        ("SparkleHome", "PureWash", "ShinePro"),
        (
            "Dishwash Liquid",
            "Detergent Powder",
            "Floor Cleaner",
            "Toilet Cleaner",
            "Garbage Bags",
            "Kitchen Towels",
            "Glass Cleaner",
        ),
        ("500 ml", "1 l", "1 kg", "30 pcs"),
    ),
    CategorySpec(
        "Personal Care",
        False,
        0.06,
        (45, 520),
        1.1,
        0.9,
        ("PureGlow", "UrbanGroom", "HerbaCare"),
        (
            "Shampoo",
            "Body Wash",
            "Toothpaste",
            "Face Wash",
            "Deodorant",
            "Hand Wash",
            "Moisturiser",
            "Sanitary Pads",
        ),
        ("100 ml", "180 ml", "250 ml", "400 ml"),
    ),
    CategorySpec(
        "Baby Care",
        False,
        0.03,
        (85, 950),
        1.1,
        1.0,
        ("TinyTots", "BabyBloom"),
        ("Diapers", "Baby Wipes", "Baby Lotion", "Baby Food", "Baby Shampoo"),
        ("1 pack", "72 pcs", "200 ml", "300 g"),
    ),
    CategorySpec(
        "Meat & Seafood",
        True,
        0.05,
        (120, 620),
        0.8,
        1.2,
        ("SeaHarvest", "PrimeCuts"),
        (
            "Chicken Curry Cut",
            "Chicken Breast",
            "Mutton Curry Cut",
            "Prawns",
            "Rohu Fish",
            "Chicken Sausages",
        ),
        ("250 g", "450 g", "500 g", "1 kg"),
    ),
    CategorySpec(
        "Pet Care",
        False,
        0.02,
        (95, 820),
        1.0,
        1.0,
        ("PawPal", "WhiskerWay"),
        ("Dog Food", "Cat Food", "Pet Treats", "Cat Litter"),
        ("400 g", "1 kg", "3 kg"),
    ),
    CategorySpec(
        "Pharma & Wellness",
        False,
        0.03,
        (30, 420),
        1.0,
        1.1,
        ("WellCare", "VitaPlus"),
        ("Paracetamol Strip", "ORS Sachets", "Multivitamin", "Bandages", "Antiseptic Liquid", "Cough Syrup"),
        ("10 pcs", "4 pcs", "60 pcs", "100 ml"),
    ),
)

VEHICLES: tuple[tuple[str, float, float], ...] = (
    # (vehicle_type, share of fleet, average speed km/h)
    ("motorbike", 0.62, 26.0),
    ("ev_scooter", 0.30, 23.0),
    ("bicycle", 0.08, 14.0),
)

PAYMENT_METHODS: tuple[tuple[str, float], ...] = (
    ("upi", 0.58),
    ("card", 0.14),
    ("wallet", 0.10),
    ("cod", 0.18),
)

# Days with demand spikes (IST calendar dates, MM-DD) and their multipliers.
FESTIVALS: dict[str, float] = {
    "01-01": 1.35,  # New Year's Day
    "01-14": 1.20,  # Makar Sankranti / Pongal
    "03-04": 1.45,  # Holi (2026)
    "08-15": 1.25,  # Independence Day
    "08-28": 1.40,  # Raksha Bandhan (2026)
    "09-14": 1.30,  # Ganesh Chaturthi (2026)
    "10-20": 1.35,  # Dussehra (2026)
    "11-07": 1.60,  # Diwali eve (2026)
    "11-08": 1.75,  # Diwali (2026)
    "12-25": 1.30,  # Christmas
    "12-31": 1.85,  # New Year's Eve
}

ROLLING_FLASH_PROMO_PREFIX = "FLASH"
