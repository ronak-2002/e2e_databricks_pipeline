import copy
import logging
import random
import threading
import time
import uuid
from typing import Dict, List, Optional

from catalog import catalog
from config import config
from event_builder import event_builder
from kafka_producer import kafka_producer
from state_store import state_store

logger = logging.getLogger("TrafficSimulator")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [Simulator] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Search queries used by simulated shoppers
SIMULATED_SEARCH_QUERIES = [
    "headphones", "4k monitor", "mechanical keyboard", "fleece jacket",
    "denim jeans", "running shoes", "coffee maker", "cast iron",
    "ergonomic chair", "hepa filter", "designing data intensive",
    "clean architecture", "kafka streaming", "blender", "smart watch",
]


class VirtualShopperBot:
    """Represents an autonomous virtual user executing a stateful journey."""

    def __init__(self, bot_id: int):
        self.bot_id = bot_id
        self.state = "IDLE"
        self.persona: dict = {}
        self.session_id: str = ""
        self.user_id: str = ""
        self.device_type: str = "web"
        self.current_product: Optional[dict] = None
        self.action_count: int = 0
        self.last_action: str = "Initialized"
        self.reset_identity()

    def reset_identity(self):
        """Assign a fresh identity and session for a new user journey."""
        self.persona = catalog.generate_random_user()
        self.user_id = self.persona["user_id"]
        self.session_id = f"sess_bot_{self.bot_id}_{uuid.uuid4().hex[:8]}"
        self.device_type = self.persona["device_type"]
        self.state = "IDLE"
        self.current_product = None
        self.last_action = "Assigned fresh identity"
        # Register in state store
        state_store.get_or_create_session(
            session_id=self.session_id,
            user_id=self.user_id,
            device_type=self.device_type,
            user_meta=self.persona,
        )

    def to_dict(self) -> dict:
        return {
            "bot_id": self.bot_id,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "name": self.persona.get("name", "Simulated Bot"),
            "avatar": self.persona.get("avatar", "🤖"),
            "device_type": self.device_type,
            "state": self.state,
            "last_action": self.last_action,
            "action_count": self.action_count,
        }


class TrafficSimulator:
    """
    Background multi-session stateful traffic simulator.
    Simulates concurrent realistic user journeys following a Markov-style state machine:
    IDLE -> LOGIN -> BROWSING -> SEARCHING -> VIEW_PRODUCT -> ADD_TO_CART
         -> (REMOVE/CLEAR) -> CHECKOUT -> PLACE_ORDER -> LOGOUT -> IDLE
    """

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()

        self.tick_interval: float = config.SIMULATOR_TICK_SECONDS
        self.num_bots: int = config.SIMULATOR_NUM_USERS
        self._bots: List[VirtualShopperBot] = []
        self._bot_round_robin_idx: int = 0
        self._total_ticks: int = 0

        self._init_bots()

    def _init_bots(self):
        """Initialize or adjust virtual shopper bots."""
        with self._lock:
            current_count = len(self._bots)
            if self.num_bots > current_count:
                for i in range(current_count, self.num_bots):
                    self._bots.append(VirtualShopperBot(bot_id=i + 1))
            elif self.num_bots < current_count:
                self._bots = self._bots[:self.num_bots]

    def set_num_users(self, n: int):
        """Dynamically set the number of concurrent simulated bots (1-10)."""
        with self._lock:
            n = max(1, min(10, int(n)))
            self.num_bots = n
            self._init_bots()

    def set_speed(self, seconds: float):
        """Dynamically adjust tick interval (0.2s - 5.0s)."""
        with self._lock:
            self.tick_interval = max(0.2, min(5.0, float(seconds)))

    def is_running(self) -> bool:
        return self._running

    def start(self):
        """Start the background simulation thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, name="TrafficSimulatorThread", daemon=True)
            self._thread.start()
            logger.info(f"Simulator started with {self.num_bots} bots, tick speed {self.tick_interval}s.")

    def stop(self):
        """Stop the background simulation thread."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("Simulator stopped.")

    def _run_loop(self):
        """Main loop executing state transitions on intervals."""
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception as e:
                logger.error(f"Error in simulator tick: {e}", exc_info=True)

            # Sleep in small slices to respond promptly to stop events
            sleep_needed = self.tick_interval
            step = 0.1
            while sleep_needed > 0 and not self._stop_event.is_set():
                time.sleep(min(step, sleep_needed))
                sleep_needed -= step

    def tick(self) -> Optional[dict]:
        """
        Advance one virtual shopper bot through its Markov state machine.
        Returns the emitted event envelope if one was emitted.
        """
        with self._lock:
            if not self._bots:
                return None

            # Pick next bot round-robin
            bot = self._bots[self._bot_round_robin_idx % len(self._bots)]
            self._bot_round_robin_idx += 1
            self._total_ticks += 1

            event_envelope = self._advance_bot_state(bot)
            return event_envelope

    def _advance_bot_state(self, bot: VirtualShopperBot) -> Optional[dict]:
        """Execute the next state machine action for the given bot."""
        current_state = bot.state
        bot.action_count += 1
        emitted_event = None

        # ---------------------------------------------------------------------
        # 1. IDLE -> LOGIN
        # ---------------------------------------------------------------------
        if current_state == "IDLE":
            payload = {
                "user_name": bot.persona["name"],
                "email": bot.persona["email"],
                "login_method": "cookie_remember_me",
                "ip_address": f"198.51.100.{bot.bot_id}",
            }
            emitted_event = self._emit(
                event_type="user_login",
                bot=bot,
                payload=payload,
            )
            bot.last_action = f"Logged in as {bot.persona['name']}"
            bot.state = random.choice(["BROWSING", "SEARCHING"])

        # ---------------------------------------------------------------------
        # 2. BROWSING Category
        # ---------------------------------------------------------------------
        elif current_state == "BROWSING":
            category = random.choice(catalog.get_categories())
            payload = {
                "category": category,
                "referrer": "home_page",
                "filter_applied": random.choice(["top_rated", "price_asc", "newest", "none"]),
            }
            emitted_event = self._emit(
                event_type="category_browsed",
                bot=bot,
                payload=payload,
            )
            bot.last_action = f"Browsing category: {category}"
            # Next state: 65% view product, 20% search, 15% logout
            roll = random.random()
            if roll < 0.65:
                bot.state = "VIEW_PRODUCT"
                bot.current_product = catalog.get_random_product(category)
            elif roll < 0.85:
                bot.state = "SEARCHING"
            else:
                bot.state = "LOGOUT"

        # ---------------------------------------------------------------------
        # 3. SEARCHING Catalog
        # ---------------------------------------------------------------------
        elif current_state == "SEARCHING":
            query = random.choice(SIMULATED_SEARCH_QUERIES)
            search_results = catalog.get_all(search=query)
            payload = {
                "query": query,
                "results_count": len(search_results),
                "auto_suggest": random.choice([True, False]),
            }
            emitted_event = self._emit(
                event_type="product_searched",
                bot=bot,
                payload=payload,
            )
            bot.last_action = f"Searched for '{query}' ({len(search_results)} hits)"
            # Next state: 75% view product from search, 15% browse category, 10% logout
            roll = random.random()
            if roll < 0.75 and search_results:
                bot.state = "VIEW_PRODUCT"
                bot.current_product = random.choice(search_results)
            elif roll < 0.90:
                bot.state = "BROWSING"
            else:
                bot.state = "LOGOUT"

        # ---------------------------------------------------------------------
        # 4. VIEW_PRODUCT Details
        # ---------------------------------------------------------------------
        elif current_state == "VIEW_PRODUCT":
            if not bot.current_product:
                bot.current_product = catalog.get_random_product()

            prod = bot.current_product
            payload = {
                "product_id": prod["id"],
                "product_name": prod["name"],
                "category": prod["category"],
                "price": prod["price"],
                "stock": prod["stock"],
                "view_duration_seconds": random.randint(3, 45),
            }
            emitted_event = self._emit(
                event_type="product_viewed",
                bot=bot,
                payload=payload,
            )
            bot.last_action = f"Viewed {prod['name']} (${prod['price']})"
            # Next state: 55% add to cart, 25% browse another, 10% view another product, 10% logout
            roll = random.random()
            if roll < 0.55:
                bot.state = "ADD_TO_CART"
            elif roll < 0.80:
                bot.state = "BROWSING"
            elif roll < 0.90:
                bot.current_product = catalog.get_random_product(prod["category"])
                bot.state = "VIEW_PRODUCT"
            else:
                bot.state = "LOGOUT"

        # ---------------------------------------------------------------------
        # 5. ADD_TO_CART
        # ---------------------------------------------------------------------
        elif current_state == "ADD_TO_CART":
            if not bot.current_product:
                bot.current_product = catalog.get_random_product()

            prod = bot.current_product
            qty = random.choices([1, 2, 3], weights=[0.8, 0.15, 0.05])[0]
            updated_cart = state_store.add_to_cart(bot.session_id, prod, quantity=qty)

            payload = {
                "product_id": prod["id"],
                "product_name": prod["name"],
                "category": prod["category"],
                "price": prod["price"],
                "quantity_added": qty,
                "cart_subtotal": updated_cart["subtotal"],
                "cart_item_count": updated_cart["item_count"],
            }
            emitted_event = self._emit(
                event_type="cart_item_added",
                bot=bot,
                payload=payload,
            )
            bot.last_action = f"Added {qty}x {prod['name']} to cart"
            # Next state: 45% proceed to checkout, 35% browse/view another, 10% remove, 10% clear
            roll = random.random()
            if roll < 0.45:
                bot.state = "CHECKOUT_START"
            elif roll < 0.80:
                bot.state = "VIEW_PRODUCT"
                bot.current_product = catalog.get_random_product()
            elif roll < 0.90:
                bot.state = "REMOVE_FROM_CART"
            else:
                bot.state = "CLEAR_CART"

        # ---------------------------------------------------------------------
        # 6. REMOVE_FROM_CART
        # ---------------------------------------------------------------------
        elif current_state == "REMOVE_FROM_CART":
            cart = state_store.get_cart(bot.session_id)
            if cart["items"]:
                item_to_remove = random.choice(cart["items"])
                updated_cart = state_store.remove_from_cart(bot.session_id, item_to_remove["product_id"])
                payload = {
                    "product_id": item_to_remove["product_id"],
                    "product_name": item_to_remove["name"],
                    "quantity_removed": item_to_remove["quantity"],
                    "remaining_subtotal": updated_cart["subtotal"],
                }
                emitted_event = self._emit(
                    event_type="cart_item_removed",
                    bot=bot,
                    payload=payload,
                )
                bot.last_action = f"Removed {item_to_remove['name']} from cart"
            # Next: 50% checkout (if cart has items), else browse
            cart = state_store.get_cart(bot.session_id)
            if cart["items"] and random.random() < 0.6:
                bot.state = "CHECKOUT_START"
            else:
                bot.state = "BROWSING"

        # ---------------------------------------------------------------------
        # 7. CLEAR_CART
        # ---------------------------------------------------------------------
        elif current_state == "CLEAR_CART":
            state_store.clear_cart(bot.session_id)
            payload = {"reason": "shopper_changed_mind"}
            emitted_event = self._emit(
                event_type="cart_cleared",
                bot=bot,
                payload=payload,
            )
            bot.last_action = "Cleared shopping cart"
            bot.state = random.choice(["BROWSING", "LOGOUT"])

        # ---------------------------------------------------------------------
        # 8. CHECKOUT_START
        # ---------------------------------------------------------------------
        elif current_state == "CHECKOUT_START":
            cart = state_store.get_cart(bot.session_id)
            if not cart["items"]:
                bot.state = "BROWSING"
                bot.last_action = "Cart was empty, resumed browsing"
                return None

            payload = {
                "cart_subtotal": cart["subtotal"],
                "item_count": cart["item_count"],
                "shipping_country": "US",
                "express_checkout": random.choice([True, False]),
            }
            emitted_event = self._emit(
                event_type="checkout_started",
                bot=bot,
                payload=payload,
            )
            bot.last_action = f"Initiated checkout for ${cart['subtotal']}"
            # 85% complete order, 15% abandon checkout
            if random.random() < 0.85:
                bot.state = "PLACE_ORDER"
            else:
                bot.state = "BROWSING"

        # ---------------------------------------------------------------------
        # 9. PLACE_ORDER & PAYMENT
        # ---------------------------------------------------------------------
        elif current_state == "PLACE_ORDER":
            cart = state_store.get_cart(bot.session_id)
            if not cart["items"]:
                bot.state = "IDLE"
                bot.reset_identity()
                return None

            # First emit payment_processed
            payment_method = random.choice(["credit_card", "apple_pay", "google_pay", "paypal"])
            pay_payload = {
                "amount": cart["subtotal"],
                "currency": "USD",
                "payment_method": payment_method,
                "status": "SUCCESS",
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            }
            self._emit(
                event_type="payment_processed",
                bot=bot,
                payload=pay_payload,
            )

            # Create order in state store
            order = state_store.create_order(
                session_id=bot.session_id,
                user_id=bot.user_id,
                shipping_info={"city": bot.persona["city"], "country": "US"},
                payment_info={"method": payment_method, "last4": str(random.randint(1000, 9999))},
            )

            order_payload = {
                "order_id": order["order_id"],
                "total_amount": order["total"],
                "items_count": len(order["items"]),
                "shipping_city": bot.persona["city"],
                "items_summary": [
                    {"product_id": i["product_id"], "name": i["name"], "qty": i["quantity"]}
                    for i in order["items"]
                ],
            }
            emitted_event = self._emit(
                event_type="order_placed",
                bot=bot,
                payload=order_payload,
            )
            bot.last_action = f"🎉 Placed order {order['order_id']} (${order['total']})"
            bot.state = "LOGOUT"

        # ---------------------------------------------------------------------
        # 10. LOGOUT
        # ---------------------------------------------------------------------
        elif current_state == "LOGOUT":
            payload = {
                "duration_minutes": random.randint(2, 28),
                "reason": "user_exit",
            }
            emitted_event = self._emit(
                event_type="user_logout",
                bot=bot,
                payload=payload,
            )
            bot.last_action = "Logged out"
            bot.reset_identity()  # assign new identity for next cycle
            bot.state = "IDLE"

        return emitted_event

    def _emit(self, event_type: str, bot: VirtualShopperBot, payload: dict) -> dict:
        """Build event envelope, route topic, and emit via kafka_producer."""
        envelope, topic = event_builder.build_event(
            event_type=event_type,
            user_id=bot.user_id,
            session_id=bot.session_id,
            payload=payload,
            device_type=bot.device_type,
            source="auto_simulator",
            app_version=config.APP_VERSION,
        )
        kafka_producer.send(topic=topic, event=envelope, key=bot.user_id)
        return envelope

    def get_status(self) -> dict:
        """Return simulator telemetry, running status, and bot journey states."""
        with self._lock:
            return {
                "is_running": self._running,
                "num_users": self.num_bots,
                "tick_seconds": self.tick_interval,
                "total_ticks": self._total_ticks,
                "bots": [bot.to_dict() for bot in self._bots],
            }


# Global singleton simulator
simulator = TrafficSimulator()
