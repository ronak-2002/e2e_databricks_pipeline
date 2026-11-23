import copy
import threading
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class StateStore:
    """
    Thread-safe in-memory state manager tracking active user sessions,
    shopping carts, completed orders, and a rolling ring buffer of the
    last 100 emitted events for live observability.
    """

    def __init__(self, max_event_buffer: int = 100):
        self._lock = threading.RLock()
        self._max_event_buffer = max_event_buffer
        self._sessions: Dict[str, dict] = {}
        self._carts: Dict[str, dict] = {}
        self._orders: Dict[str, dict] = {}
        self._event_buffer: deque = deque(maxlen=max_event_buffer)
        self._event_seq: int = 0
        self._created_at = datetime.now(timezone.utc).isoformat()

        # Metrics counters
        self._total_events_emitted: int = 0
        self._events_by_topic: Dict[str, int] = {}
        self._events_by_type: Dict[str, int] = {}
        self._events_by_source: Dict[str, int] = {"manual_ui": 0, "auto_simulator": 0}

    # -------------------------------------------------------------------------
    # Session Management
    # -------------------------------------------------------------------------
    def get_or_create_session(
        self,
        session_id: Optional[str] = None,
        user_id: Optional[str] = None,
        device_type: str = "web",
        user_meta: Optional[dict] = None,
    ) -> dict:
        """Get existing session or create a new active session."""
        with self._lock:
            now_iso = datetime.now(timezone.utc).isoformat()
            sid = session_id or f"sess_{uuid.uuid4().hex[:12]}"
            uid = user_id or f"usr_{uuid.uuid4().hex[:8]}"

            if sid in self._sessions:
                sess = self._sessions[sid]
                sess["last_active"] = now_iso
                if user_id:
                    sess["user_id"] = user_id
                if device_type:
                    sess["device_type"] = device_type
                if user_meta:
                    sess.update(user_meta)
                return copy.deepcopy(sess)

            # New session
            session_data = {
                "session_id": sid,
                "user_id": uid,
                "device_type": device_type,
                "created_at": now_iso,
                "last_active": now_iso,
                "name": (user_meta or {}).get("name", "Guest Shopper"),
                "email": (user_meta or {}).get("email", ""),
                "avatar": (user_meta or {}).get("avatar", "👤"),
            }
            if user_meta:
                session_data.update(user_meta)
            self._sessions[sid] = session_data

            # Initialize empty cart for this session
            if sid not in self._carts:
                self._carts[sid] = {
                    "session_id": sid,
                    "items": [],
                    "subtotal": 0.0,
                    "item_count": 0,
                    "updated_at": now_iso,
                }

            return copy.deepcopy(session_data)

    def get_session(self, session_id: str) -> Optional[dict]:
        with self._lock:
            sess = self._sessions.get(session_id)
            return copy.deepcopy(sess) if sess else None

    # -------------------------------------------------------------------------
    # Cart Management
    # -------------------------------------------------------------------------
    def get_cart(self, session_id: str) -> dict:
        with self._lock:
            if session_id not in self._carts:
                self._carts[session_id] = {
                    "session_id": session_id,
                    "items": [],
                    "subtotal": 0.0,
                    "item_count": 0,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            return copy.deepcopy(self._carts[session_id])

    def add_to_cart(self, session_id: str, product: dict, quantity: int = 1) -> dict:
        """Add product to cart or increment quantity."""
        with self._lock:
            cart = self._carts.setdefault(
                session_id,
                {
                    "session_id": session_id,
                    "items": [],
                    "subtotal": 0.0,
                    "item_count": 0,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

            p_id = product["id"]
            price = float(product["price"])
            qty = max(1, int(quantity))

            # Check if item exists in cart
            existing_item = next((item for item in cart["items"] if item["product_id"] == p_id), None)
            if existing_item:
                existing_item["quantity"] += qty
                existing_item["total"] = round(existing_item["quantity"] * price, 2)
            else:
                cart["items"].append({
                    "product_id": p_id,
                    "name": product["name"],
                    "price": price,
                    "category": product.get("category", "General"),
                    "image_icon": product.get("image_icon", "📦"),
                    "quantity": qty,
                    "total": round(qty * price, 2),
                })

            self._recalculate_cart(cart)
            return copy.deepcopy(cart)

    def remove_from_cart(self, session_id: str, product_id: str, quantity: Optional[int] = None) -> dict:
        """Remove product or decrement quantity."""
        with self._lock:
            cart = self._carts.get(session_id)
            if not cart:
                return {"session_id": session_id, "items": [], "subtotal": 0.0, "item_count": 0}

            item = next((i for i in cart["items"] if i["product_id"] == product_id), None)
            if item:
                if quantity is not None and int(quantity) > 0 and item["quantity"] > quantity:
                    item["quantity"] -= int(quantity)
                    item["total"] = round(item["quantity"] * item["price"], 2)
                else:
                    cart["items"] = [i for i in cart["items"] if i["product_id"] != product_id]

            self._recalculate_cart(cart)
            return copy.deepcopy(cart)

    def clear_cart(self, session_id: str) -> dict:
        """Empty the cart."""
        with self._lock:
            cart = self._carts.setdefault(
                session_id,
                {"session_id": session_id, "items": [], "subtotal": 0.0, "item_count": 0},
            )
            cart["items"] = []
            self._recalculate_cart(cart)
            return copy.deepcopy(cart)

    def _recalculate_cart(self, cart: dict):
        subtotal = sum(item["price"] * item["quantity"] for item in cart["items"])
        item_count = sum(item["quantity"] for item in cart["items"])
        cart["subtotal"] = round(subtotal, 2)
        cart["item_count"] = item_count
        cart["updated_at"] = datetime.now(timezone.utc).isoformat()

    # -------------------------------------------------------------------------
    # Order Placement
    # -------------------------------------------------------------------------
    def create_order(
        self,
        session_id: str,
        user_id: str,
        shipping_info: Optional[dict] = None,
        payment_info: Optional[dict] = None,
    ) -> dict:
        """Convert cart contents into a completed order."""
        with self._lock:
            cart = self.get_cart(session_id)
            if not cart["items"]:
                raise ValueError("Cannot place an order with an empty cart.")

            order_id = f"ord_{uuid.uuid4().hex[:10]}"
            now_iso = datetime.now(timezone.utc).isoformat()

            order_data = {
                "order_id": order_id,
                "session_id": session_id,
                "user_id": user_id,
                "items": copy.deepcopy(cart["items"]),
                "subtotal": cart["subtotal"],
                "tax": round(cart["subtotal"] * 0.08, 2),
                "shipping_fee": 0.0 if cart["subtotal"] >= 100.0 else 9.99,
                "total": round(
                    cart["subtotal"] + (cart["subtotal"] * 0.08) + (0.0 if cart["subtotal"] >= 100.0 else 9.99),
                    2,
                ),
                "status": "CONFIRMED",
                "created_at": now_iso,
                "shipping": shipping_info or {
                    "address": "100 Market St",
                    "city": "San Francisco",
                    "state": "CA",
                    "zip": "94105",
                },
                "payment": {
                    "method": (payment_info or {}).get("method", "credit_card"),
                    "last4": (payment_info or {}).get("last4", "4242"),
                    "status": "CAPTURED",
                },
            }

            self._orders[order_id] = order_data
            # Clear cart upon order placement
            self.clear_cart(session_id)
            return copy.deepcopy(order_data)

    def get_order(self, order_id: str) -> Optional[dict]:
        with self._lock:
            order = self._orders.get(order_id)
            return copy.deepcopy(order) if order else None

    # -------------------------------------------------------------------------
    # Rolling Event Buffer & Live Inspector
    # -------------------------------------------------------------------------
    def record_event(self, envelope: dict, topic: str):
        """Append an emitted event to the rolling 100-event ring buffer and update stats."""
        with self._lock:
            self._event_seq += 1
            entry = {
                "seq": self._event_seq,
                "topic": topic,
                "event": copy.deepcopy(envelope),
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
            self._event_buffer.append(entry)

            # Update metrics
            self._total_events_emitted += 1
            self._events_by_topic[topic] = self._events_by_topic.get(topic, 0) + 1
            etype = envelope.get("event_type", "unknown")
            self._events_by_type[etype] = self._events_by_type.get(etype, 0) + 1
            src = envelope.get("source", "unknown")
            self._events_by_source[src] = self._events_by_source.get(src, 0) + 1

    def get_recent_events(
        self,
        limit: int = 50,
        since_seq: Optional[int] = None,
        topic: Optional[str] = None,
        source: Optional[str] = None,
    ) -> List[dict]:
        """Fetch recent events from the rolling buffer with optional filtering."""
        with self._lock:
            events = list(self._event_buffer)

            if since_seq is not None:
                events = [e for e in events if e["seq"] > since_seq]

            if topic and topic.strip() and topic.lower() != "all":
                events = [e for e in events if e["topic"].lower() == topic.strip().lower()]

            if source and source.strip() and source.lower() != "all":
                events = [e for e in events if e["event"].get("source", "").lower() == source.strip().lower()]

            # Return the latest first
            events = events[-limit:]
            return copy.deepcopy(events)

    def get_stats(self) -> dict:
        """Return real-time metrics summary."""
        with self._lock:
            return {
                "total_events_emitted": self._total_events_emitted,
                "current_buffer_size": len(self._event_buffer),
                "max_buffer_size": self._max_event_buffer,
                "latest_seq": self._event_seq,
                "active_sessions_count": len(self._sessions),
                "total_orders_count": len(self._orders),
                "events_by_topic": dict(self._events_by_topic),
                "events_by_type": dict(self._events_by_type),
                "events_by_source": dict(self._events_by_source),
                "uptime_started_at": self._created_at,
            }

    def reset(self):
        """Reset all in-memory state (useful for tests and demo resets)."""
        with self._lock:
            self._sessions.clear()
            self._carts.clear()
            self._orders.clear()
            self._event_buffer.clear()
            self._event_seq = 0
            self._total_events_emitted = 0
            self._events_by_topic.clear()
            self._events_by_type.clear()
            self._events_by_source = {"manual_ui": 0, "auto_simulator": 0}
            self._created_at = datetime.now(timezone.utc).isoformat()


# Global singleton instance
state_store = StateStore(max_event_buffer=100)
