import uuid
from datetime import datetime, timezone
from typing import Dict, Tuple
from config import config

# Valid allowed device types and event sources
ALLOWED_DEVICE_TYPES = {"web", "ios", "android"}
ALLOWED_SOURCES = {"manual_ui", "auto_simulator"}

# Canonical mapping from event_type to target Kafka topic (Strictly 4 topics max)
TOPIC_ROUTING_MAP: Dict[str, str] = {
    # 1. ecom.users (Identity & Session Lifecycle)
    "user_registered": config.TOPIC_USERS,
    "user_login": config.TOPIC_USERS,
    "user_logout": config.TOPIC_USERS,

    # 2. ecom.browse (Discovery, Search & View)
    "product_viewed": config.TOPIC_BROWSE,
    "product_searched": config.TOPIC_BROWSE,
    "category_browsed": config.TOPIC_BROWSE,

    # 3. ecom.cart (Basket Intent & Item Management)
    "cart_item_added": config.TOPIC_CART,
    "cart_item_removed": config.TOPIC_CART,
    "cart_cleared": config.TOPIC_CART,

    # 4. ecom.orders (End-to-End Order & Payment Funnel)
    "checkout_started": config.TOPIC_ORDERS,
    "checkout_step_completed": config.TOPIC_ORDERS,
    "payment_processed": config.TOPIC_ORDERS,
    "order_placed": config.TOPIC_ORDERS,
}


class EventValidationError(ValueError):
    """Raised when an event does not adhere to the strict JSON envelope schema."""
    pass


class EventBuilder:
    """Builds and strictly validates e-commerce events and routes them to Kafka topics."""

    @staticmethod
    def get_topic_for_event_type(event_type: str) -> str:
        """Route an event type to its designated Kafka topic (strictly 4 topics max)."""
        return TOPIC_ROUTING_MAP.get(event_type, config.TOPIC_ORDERS)

    @staticmethod
    def build_event(
        event_type: str,
        user_id: str,
        session_id: str,
        payload: dict,
        device_type: str = "web",
        source: str = "manual_ui",
        app_version: str = config.APP_VERSION,
        event_id: str = None,
        event_timestamp: str = None,
    ) -> Tuple[dict, str]:
        """
        Build an event following the exact required schema:
        {
          "event_id": "<uuid4>",
          "event_type": "<string>",
          "event_timestamp": "<ISO-8601 UTC string>",
          "user_id": "<string>",
          "session_id": "<string>",
          "device_type": "web | ios | android",
          "app_version": "2.4.1",
          "source": "manual_ui | auto_simulator",
          "payload": { }
        }
        Returns: (event_dict, target_topic)
        """
        if not event_type or not isinstance(event_type, str):
            raise EventValidationError("event_type must be a non-empty string.")

        if not user_id or not isinstance(user_id, str):
            raise EventValidationError("user_id must be a non-empty string.")

        if not session_id or not isinstance(session_id, str):
            raise EventValidationError("session_id must be a non-empty string.")

        device_lower = str(device_type).lower()
        if device_lower not in ALLOWED_DEVICE_TYPES:
            raise EventValidationError(
                f"Invalid device_type '{device_type}'. Must be one of: {sorted(ALLOWED_DEVICE_TYPES)}"
            )

        source_lower = str(source).lower()
        if source_lower not in ALLOWED_SOURCES:
            raise EventValidationError(
                f"Invalid source '{source}'. Must be one of: {sorted(ALLOWED_SOURCES)}"
            )

        if not isinstance(payload, dict):
            raise EventValidationError("payload must be a dictionary object.")

        envelope = {
            "event_id": event_id or str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": event_timestamp or datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "session_id": session_id,
            "device_type": device_lower,
            "app_version": app_version or config.APP_VERSION,
            "source": source_lower,
            "payload": payload,
        }

        EventBuilder.validate_event(envelope)
        topic = EventBuilder.get_topic_for_event_type(event_type)
        return envelope, topic

    @staticmethod
    def validate_event(envelope: dict) -> bool:
        """Strict validation of the event envelope fields and types."""
        required_keys = {
            "event_id",
            "event_type",
            "event_timestamp",
            "user_id",
            "session_id",
            "device_type",
            "app_version",
            "source",
            "payload",
        }
        missing = required_keys - set(envelope.keys())
        if missing:
            raise EventValidationError(f"Missing required envelope fields: {sorted(missing)}")

        if not isinstance(envelope["event_id"], str) or len(envelope["event_id"]) < 8:
            raise EventValidationError("event_id must be a valid non-empty UUID string.")

        if not isinstance(envelope["event_type"], str) or not envelope["event_type"].strip():
            raise EventValidationError("event_type must be a non-empty string.")

        if not isinstance(envelope["event_timestamp"], str) or not envelope["event_timestamp"].strip():
            raise EventValidationError("event_timestamp must be a valid ISO-8601 string.")

        if envelope["device_type"] not in ALLOWED_DEVICE_TYPES:
            raise EventValidationError(f"Invalid device_type: {envelope['device_type']}")

        if envelope["source"] not in ALLOWED_SOURCES:
            raise EventValidationError(f"Invalid source: {envelope['source']}")

        if not isinstance(envelope["payload"], dict):
            raise EventValidationError("payload must be a dictionary.")

        return True


event_builder = EventBuilder()
