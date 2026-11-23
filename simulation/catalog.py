import random
from typing import Dict, List, Optional
from faker import Faker

fake = Faker()
Faker.seed(42)

# Curated catalog of 32 realistic products across 4 popular e-commerce categories
CATALOG_ITEMS = [
    # Electronics (8)
    {
        "id": "prod-el-001",
        "name": "Sony WH-1000XM5 Wireless Headphones",
        "category": "Electronics",
        "price": 398.00,
        "rating": 4.8,
        "reviews_count": 2420,
        "stock": 45,
        "image_icon": "🎧",
        "description": "Industry-leading noise cancellation with two processors and 8 microphones for superior call quality.",
        "tags": ["audio", "wireless", "noise-canceling", "bluetooth"],
    },
    {
        "id": "prod-el-002",
        "name": "Dell UltraSharp 32 4K USB-C Hub Monitor",
        "category": "Electronics",
        "price": 789.99,
        "rating": 4.7,
        "reviews_count": 890,
        "stock": 20,
        "image_icon": "🖥️",
        "description": "Brilliant 4K UHD resolution with IPS Black technology, 98% DCI-P3 color gamut, and 90W power delivery.",
        "tags": ["monitor", "4k", "display", "productivity"],
    },
    {
        "id": "prod-el-003",
        "name": "Keychron Q1 Pro Wireless Mechanical Keyboard",
        "category": "Electronics",
        "price": 199.00,
        "rating": 4.9,
        "reviews_count": 1340,
        "stock": 32,
        "image_icon": "⌨️",
        "description": "Full aluminum CNC body, hot-swappable switches, double-gasket design, and customizable QMK/VIA firmware.",
        "tags": ["keyboard", "mechanical", "custom", "accessories"],
    },
    {
        "id": "prod-el-004",
        "name": "Anker Prime 100W GaN Wall Charger",
        "category": "Electronics",
        "price": 69.99,
        "rating": 4.8,
        "reviews_count": 4210,
        "stock": 110,
        "image_icon": "🔌",
        "description": "Ultra-compact 3-port fast charger with PowerIQ 4.0 dynamic power distribution for laptops and phones.",
        "tags": ["charger", "gan", "usb-c", "travel"],
    },
    {
        "id": "prod-el-005",
        "name": "Apple Watch Ultra 2 Titanium",
        "category": "Electronics",
        "price": 799.00,
        "rating": 4.9,
        "reviews_count": 3120,
        "stock": 25,
        "image_icon": "⌚",
        "description": "Rugged 49mm titanium case, precision dual-frequency GPS, up to 36 hours of battery life, and 100m water resistance.",
        "tags": ["wearable", "smartwatch", "fitness", "outdoor"],
    },
    {
        "id": "prod-el-006",
        "name": "Sonos Move 2 Portable Smart Speaker",
        "category": "Electronics",
        "price": 449.00,
        "rating": 4.6,
        "reviews_count": 670,
        "stock": 18,
        "image_icon": "🔊",
        "description": "Powerful stereo sound outdoors and indoors with 24-hour battery life, IP56 water resistance, and WiFi/Bluetooth.",
        "tags": ["audio", "speaker", "smart-home", "portable"],
    },
    {
        "id": "prod-el-007",
        "name": "CalDigit TS4 Thunderbolt 4 Dock",
        "category": "Electronics",
        "price": 399.95,
        "rating": 4.7,
        "reviews_count": 950,
        "stock": 14,
        "image_icon": "🗄️",
        "description": "18 ports of connectivity with 98W charging, 2.5GbE LAN, dual 4K/single 8K display support, and UHS-II card slots.",
        "tags": ["dock", "thunderbolt", "workspace", "usb-c"],
    },
    {
        "id": "prod-el-008",
        "name": "DJI Mini 4 Pro Drone with RC2",
        "category": "Electronics",
        "price": 959.00,
        "rating": 4.8,
        "reviews_count": 1540,
        "stock": 12,
        "image_icon": "🚁",
        "description": "Sub-249g folding drone with omnidirectional obstacle sensing, 4K/60fps HDR true vertical video, and 20km transmission.",
        "tags": ["drone", "camera", "video", "gadget"],
    },

    # Apparel (8)
    {
        "id": "prod-ap-001",
        "name": "Patagonia Better Sweater Fleece Jacket",
        "category": "Apparel",
        "price": 159.00,
        "rating": 4.9,
        "reviews_count": 5100,
        "stock": 65,
        "image_icon": "🧥",
        "description": "Warm, 100% recycled polyester fleece jacket dyed with a low-impact process and Fair Trade Certified sewn.",
        "tags": ["jacket", "fleece", "eco-friendly", "outdoor"],
    },
    {
        "id": "prod-ap-002",
        "name": "Levi's 511 Slim Fit Stretch Jeans",
        "category": "Apparel",
        "price": 79.50,
        "rating": 4.6,
        "reviews_count": 8700,
        "stock": 120,
        "image_icon": "👖",
        "description": "Classic modern slim cut with room to move, crafted from premium cotton blend with engineered stretch.",
        "tags": ["jeans", "denim", "pants", "casual"],
    },
    {
        "id": "prod-ap-003",
        "name": "Nike Air Zoom Pegasus 40 Running Shoes",
        "category": "Apparel",
        "price": 139.99,
        "rating": 4.7,
        "reviews_count": 4320,
        "stock": 50,
        "image_icon": "👟",
        "description": "Responsive road running shoes featuring dual Zoom Air units and engineered mesh for tuned neutral support.",
        "tags": ["shoes", "running", "footwear", "fitness"],
    },
    {
        "id": "prod-ap-004",
        "name": "Lululemon Align High-Rise Pant 25\"",
        "category": "Apparel",
        "price": 98.00,
        "rating": 4.8,
        "reviews_count": 9600,
        "stock": 80,
        "image_icon": "🧘",
        "description": "Weightless buttery-soft Nulu fabric designed for yoga and everyday comfort with four-way stretch.",
        "tags": ["activewear", "leggings", "yoga", "women"],
    },
    {
        "id": "prod-ap-005",
        "name": "Arc'teryx Atom LT Insulated Hoody",
        "category": "Apparel",
        "price": 285.00,
        "rating": 4.9,
        "reviews_count": 2890,
        "stock": 22,
        "image_icon": "🧗",
        "description": "Versatile synthetic insulated hoody with Coreloft compact insulation and breathable side stretch fleece panels.",
        "tags": ["outerwear", "hoody", "hiking", "cold-weather"],
    },
    {
        "id": "prod-ap-006",
        "name": "Smartwool Classic Edition Merino Crew Socks (3-Pack)",
        "category": "Apparel",
        "price": 64.00,
        "rating": 4.8,
        "reviews_count": 1820,
        "stock": 140,
        "image_icon": "🧦",
        "description": "Responsible merino wool socks offering natural temperature regulation, moisture wicking, and odor resistance.",
        "tags": ["socks", "merino", "essentials", "hiking"],
    },
    {
        "id": "prod-ap-007",
        "name": "Ray-Ban Classic Wayfarer Polarized Sunglasses",
        "category": "Apparel",
        "price": 210.00,
        "rating": 4.7,
        "reviews_count": 3410,
        "stock": 40,
        "image_icon": "🕶️",
        "description": "Timeless square acetate frames with crystal green G-15 polarized lenses providing 100% UV protection.",
        "tags": ["eyewear", "sunglasses", "accessories", "style"],
    },
    {
        "id": "prod-ap-008",
        "name": "Bellroy Classic Backpack Plus 24L",
        "category": "Apparel",
        "price": 189.00,
        "rating": 4.6,
        "reviews_count": 780,
        "stock": 35,
        "image_icon": "🎒",
        "description": "Clean, urban daypack made from water-resistant recycled fabric with dedicated 16\" laptop storage and lumbar support.",
        "tags": ["bag", "backpack", "commute", "travel"],
    },

    # Home & Kitchen (8)
    {
        "id": "prod-hk-001",
        "name": "Chemex Pour-Over Glass Coffeemaker (8-Cup)",
        "category": "Home & Kitchen",
        "price": 49.50,
        "rating": 4.8,
        "reviews_count": 5200,
        "stock": 60,
        "image_icon": "☕",
        "description": "Iconic non-porous borosilicate glass coffeemaker with polished wood collar and leather tie.",
        "tags": ["coffee", "pour-over", "kitchen", "brewing"],
    },
    {
        "id": "prod-hk-002",
        "name": "Lodge Pre-Seasoned Cast Iron Skillet (10.25\")",
        "category": "Home & Kitchen",
        "price": 24.90,
        "rating": 4.9,
        "reviews_count": 14500,
        "stock": 150,
        "image_icon": "🍳",
        "description": "Heirloom-quality cast iron skillet seasoned with 100% vegetable oil for even heat retention and searing.",
        "tags": ["cookware", "cast-iron", "skillet", "kitchen"],
    },
    {
        "id": "prod-hk-003",
        "name": "Herman Miller Aeron Ergonomic Chair",
        "category": "Home & Kitchen",
        "price": 1295.00,
        "rating": 4.9,
        "reviews_count": 2100,
        "stock": 15,
        "image_icon": "🪑",
        "description": "Benchmark ergonomic office chair with breathable Pellicle mesh, PostureFit SL sacral support, and adjustable arms.",
        "tags": ["furniture", "chair", "ergonomic", "workspace"],
    },
    {
        "id": "prod-hk-004",
        "name": "Dyson Purifier Cool Gen1 HEPA Air Purifier",
        "category": "Home & Kitchen",
        "price": 429.99,
        "rating": 4.7,
        "reviews_count": 1840,
        "stock": 28,
        "image_icon": "💨",
        "description": "Sealed HEPA H13 filtration captures 99.97% of particles down to 0.3 microns, purifying and cooling entire rooms.",
        "tags": ["appliance", "air-purifier", "home", "health"],
    },
    {
        "id": "prod-hk-005",
        "name": "Le Creuset Enameled Cast Iron Dutch Oven (5.5 Qt)",
        "category": "Home & Kitchen",
        "price": 420.00,
        "rating": 4.9,
        "reviews_count": 3900,
        "stock": 20,
        "image_icon": "🍲",
        "description": "Handcrafted French enameled cast iron round cocotte with shock-resistant enamel and tight-fitting lid.",
        "tags": ["cookware", "dutch-oven", "baking", "culinary"],
    },
    {
        "id": "prod-hk-006",
        "name": "Philips Hue White & Color Ambiance Starter Kit",
        "category": "Home & Kitchen",
        "price": 179.99,
        "rating": 4.6,
        "reviews_count": 4800,
        "stock": 42,
        "image_icon": "💡",
        "description": "3 smart LED bulbs (16 million colors), Hue Bridge, and Smart Button with Alexa, Apple Home, and Google Assistant support.",
        "tags": ["smart-home", "lighting", "iot", "ambient"],
    },
    {
        "id": "prod-hk-007",
        "name": "Vitamix Explorian E310 High-Performance Blender",
        "category": "Home & Kitchen",
        "price": 349.95,
        "rating": 4.8,
        "reviews_count": 3150,
        "stock": 25,
        "image_icon": "🍹",
        "description": "Variable 10-speed control, pulse function, and aircraft-grade stainless steel blades for smoothies, soups, and nut butter.",
        "tags": ["blender", "kitchen", "smoothie", "appliances"],
    },
    {
        "id": "prod-hk-008",
        "name": "Fellow Ode Gen 2 Brew Burr Grinder",
        "category": "Home & Kitchen",
        "price": 345.00,
        "rating": 4.7,
        "reviews_count": 920,
        "stock": 19,
        "image_icon": "☕",
        "description": "Precision 64mm commercial-grade flat burrs, anti-static technology, and 31 grind settings for clean, consistent pour-over brew.",
        "tags": ["coffee", "grinder", "kitchen", "specialty"],
    },

    # Books & Media (8)
    {
        "id": "prod-bk-001",
        "name": "Designing Data-Intensive Applications",
        "category": "Books & Media",
        "price": 49.99,
        "rating": 4.9,
        "reviews_count": 6800,
        "stock": 95,
        "image_icon": "📖",
        "description": "The definitive guide to the architecture, scalability, consistency, and reliability of modern distributed data systems by Martin Kleppmann.",
        "tags": ["tech-book", "data", "distributed-systems", "architecture"],
    },
    {
        "id": "prod-bk-002",
        "name": "Clean Architecture: A Craftsman's Guide",
        "category": "Books & Media",
        "price": 38.50,
        "rating": 4.7,
        "reviews_count": 3900,
        "stock": 70,
        "image_icon": "📘",
        "description": "Robert C. Martin (Uncle Bob) explores universal software architecture rules to build robust, testable, and maintainable systems.",
        "tags": ["tech-book", "software-engineering", "best-practices", "code"],
    },
    {
        "id": "prod-bk-003",
        "name": "The Pragmatic Programmer (20th Anniversary Edition)",
        "category": "Books & Media",
        "price": 44.95,
        "rating": 4.9,
        "reviews_count": 5400,
        "stock": 85,
        "image_icon": "📗",
        "description": "Essential wisdom for modern software practitioners by David Thomas and Andrew Hunt covering craftsmanship, career, and tooling.",
        "tags": ["tech-book", "programming", "career", "development"],
    },
    {
        "id": "prod-bk-004",
        "name": "Building Event-Driven Microservices",
        "category": "Books & Media",
        "price": 42.00,
        "rating": 4.8,
        "reviews_count": 1620,
        "stock": 60,
        "image_icon": "⚡",
        "description": "Leveraging event streams as systems of record with Apache Kafka and distributed stateful event processing by Adam Bellemare.",
        "tags": ["tech-book", "kafka", "event-driven", "microservices"],
    },
    {
        "id": "prod-bk-005",
        "name": "Kafka: The Definitive Guide (2nd Edition)",
        "category": "Books & Media",
        "price": 54.99,
        "rating": 4.8,
        "reviews_count": 2100,
        "stock": 55,
        "image_icon": "📙",
        "description": "Comprehensive guide to real-time stream processing, broker internals, replication, security, and Kafka Streams by Gwen Shapira et al.",
        "tags": ["tech-book", "kafka", "streaming", "data-engineering"],
    },
    {
        "id": "prod-bk-006",
        "name": "Site Reliability Engineering (Google)",
        "category": "Books & Media",
        "price": 41.50,
        "rating": 4.7,
        "reviews_count": 2700,
        "stock": 48,
        "image_icon": "🛠️",
        "description": "How Google runs production systems: SLOs, error budgets, incident response, postmortems, and distributed monitoring.",
        "tags": ["tech-book", "sre", "devops", "cloud"],
    },
    {
        "id": "prod-bk-007",
        "name": "Database Internals: A Deep Dive",
        "category": "Books & Media",
        "price": 47.95,
        "rating": 4.9,
        "reviews_count": 1450,
        "stock": 40,
        "image_icon": "📚",
        "description": "Storage engines, B-trees, LSM trees, write-ahead logs, and distributed consensus protocols explained by Alex Petrov.",
        "tags": ["tech-book", "databases", "storage", "internals"],
    },
    {
        "id": "prod-bk-008",
        "name": "System Design Interview (Volume 1 & 2)",
        "category": "Books & Media",
        "price": 59.99,
        "rating": 4.8,
        "reviews_count": 7800,
        "stock": 90,
        "image_icon": "📐",
        "description": "Step-by-step visual blueprint for designing web-scale systems, rate limiters, payment processors, and chat architectures by Alex Xu.",
        "tags": ["tech-book", "interview", "system-design", "scalability"],
    },
]


# Deterministic simulated user profiles for quick UI switching and testing
SIMULATED_PERSONAS = [
    {
        "user_id": "usr_alpha_101",
        "name": "Elena Rostova",
        "email": "elena.rostova@example.com",
        "avatar": "👩‍💻",
        "city": "Seattle, WA",
        "preferred_category": "Electronics",
        "device_type": "web",
    },
    {
        "user_id": "usr_beta_202",
        "name": "Marcus Chen",
        "email": "marcus.chen@example.com",
        "avatar": "👨‍💼",
        "city": "Austin, TX",
        "preferred_category": "Books & Media",
        "device_type": "ios",
    },
    {
        "user_id": "usr_gamma_303",
        "name": "Amina Diallo",
        "email": "amina.diallo@example.com",
        "avatar": "👩‍🎨",
        "city": "New York, NY",
        "preferred_category": "Apparel",
        "device_type": "android",
    },
    {
        "user_id": "usr_delta_404",
        "name": "Liam Gallagher",
        "email": "liam.g@example.com",
        "avatar": "👨‍🍳",
        "city": "Denver, CO",
        "preferred_category": "Home & Kitchen",
        "device_type": "web",
    },
    {
        "user_id": "usr_epsilon_505",
        "name": "Sophia Morales",
        "email": "sophia.m@example.com",
        "avatar": "👩‍🚀",
        "city": "San Francisco, CA",
        "preferred_category": "Electronics",
        "device_type": "ios",
    },
]


class CatalogManager:
    """Manages the in-memory product catalog and user generation."""

    def __init__(self):
        self._products: Dict[str, dict] = {item["id"]: item.copy() for item in CATALOG_ITEMS}
        self._categories: List[str] = sorted(list({item["category"] for item in CATALOG_ITEMS}))
        self._personas: List[dict] = SIMULATED_PERSONAS.copy()

    def get_all(self, category: Optional[str] = None, search: Optional[str] = None) -> List[dict]:
        """Return products filtered by category or search term."""
        results = list(self._products.values())
        if category and category.strip() and category.lower() != "all":
            results = [p for p in results if p["category"].lower() == category.strip().lower()]
        if search and search.strip():
            query = search.strip().lower()
            results = [
                p for p in results
                if query in p["name"].lower()
                or query in p["description"].lower()
                or any(query in tag.lower() for tag in p.get("tags", []))
            ]
        return results

    def get_by_id(self, product_id: str) -> Optional[dict]:
        """Get product by unique product_id."""
        return self._products.get(product_id)

    def get_categories(self) -> List[str]:
        """List distinct categories."""
        return self._categories

    def get_random_product(self, category: Optional[str] = None) -> dict:
        """Pick a random product from the catalog."""
        items = self.get_all(category=category)
        if not items:
            items = list(self._products.values())
        return random.choice(items)

    def get_personas(self) -> List[dict]:
        """Return list of predefined personas."""
        return self._personas

    def generate_random_user(self) -> dict:
        """Generate a realistic random user persona using Faker."""
        first_name = fake.first_name()
        last_name = fake.last_name()
        category = random.choice(self._categories)
        device = random.choice(["web", "ios", "android"])
        avatars = ["👩‍💻", "👨‍💼", "👩‍🎨", "👨‍🍳", "👩‍🚀", "👨‍🔧", "👩‍⚕️", "🧑‍🏫", "🧑‍🔬"]
        return {
            "user_id": f"usr_sim_{fake.unique.random_number(digits=5)}",
            "name": f"{first_name} {last_name}",
            "email": f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}",
            "avatar": random.choice(avatars),
            "city": f"{fake.city()}, {fake.state_abbr()}",
            "preferred_category": category,
            "device_type": device,
        }


# Singleton catalog instance
catalog = CatalogManager()
