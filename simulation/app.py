import logging
import os
import uuid
from flask import Flask, jsonify, render_template, request

from catalog import catalog
from config import config
from event_builder import event_builder
from kafka_producer import kafka_producer
from simulator import simulator
from state_store import state_store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("App")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = config.SECRET_KEY


def _get_request_metadata(req_data: dict) -> tuple:
    """Extract session_id, user_id, device_type from request payload or headers."""
    session_id = (
        req_data.get("session_id")
        or request.headers.get("X-Session-ID")
        or f"sess_manual_{uuid.uuid4().hex[:8]}"
    )
    user_id = (
        req_data.get("user_id")
        or request.headers.get("X-User-ID")
        or f"usr_manual_{uuid.uuid4().hex[:6]}"
    )
    device_type = (
        req_data.get("device_type")
        or request.headers.get("X-Device-Type")
        or "web"
    ).lower()
    if device_type not in ("web", "ios", "android"):
        device_type = "web"

    return session_id, user_id, device_type


# -----------------------------------------------------------------------------
# Frontend Route
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    """Render the single-page application storefront and simulator dashboard."""
    return render_template("index.html", config=config.to_safe_dict())


# -----------------------------------------------------------------------------
# Configuration & Catalog Endpoints
# -----------------------------------------------------------------------------
@app.route("/api/config", methods=["GET"])
def get_config():
    """Return runtime configuration (passwords masked)."""
    return jsonify({"success": True, "config": config.to_safe_dict()})


@app.route("/api/catalog", methods=["GET"])
def get_catalog():
    """List products with optional category and search filtering."""
    category = request.args.get("category")
    search = request.args.get("search")
    products = catalog.get_all(category=category, search=search)
    return jsonify({
        "success": True,
        "count": len(products),
        "categories": catalog.get_categories(),
        "products": products,
    })


@app.route("/api/catalog/<product_id>", methods=["GET"])
def get_product(product_id: str):
    """Retrieve details for a single product."""
    product = catalog.get_by_id(product_id)
    if not product:
        return jsonify({"success": False, "error": f"Product '{product_id}' not found"}), 404
    return jsonify({"success": True, "product": product})


@app.route("/api/personas", methods=["GET"])
def get_personas():
    """Return list of predefined personas for rapid manual identity switching."""
    return jsonify({"success": True, "personas": catalog.get_personas()})


# -----------------------------------------------------------------------------
# User & Session Endpoints
# -----------------------------------------------------------------------------
@app.route("/api/session", methods=["GET", "POST"])
def manage_session():
    """Get active session or initialize one."""
    data = request.get_json(silent=True) or {}
    session_id, user_id, device_type = _get_request_metadata(data)
    user_meta = data.get("user_meta")

    session = state_store.get_or_create_session(
        session_id=session_id,
        user_id=user_id,
        device_type=device_type,
        user_meta=user_meta,
    )
    cart = state_store.get_cart(session["session_id"])
    return jsonify({"success": True, "session": session, "cart": cart})


@app.route("/api/user/switch", methods=["POST"])
def switch_user():
    """Switch active user profile in manual UI and emit login/logout events."""
    data = request.get_json(silent=True) or {}
    session_id, user_id, device_type = _get_request_metadata(data)
    persona = data.get("persona") or {}

    # Update or create session
    session = state_store.get_or_create_session(
        session_id=session_id,
        user_id=persona.get("user_id", user_id),
        device_type=persona.get("device_type", device_type),
        user_meta=persona,
    )

    # Emit user_login event
    envelope, topic = event_builder.build_event(
        event_type="user_login",
        user_id=session["user_id"],
        session_id=session["session_id"],
        device_type=session["device_type"],
        source="manual_ui",
        payload={
            "user_name": persona.get("name", "Guest"),
            "email": persona.get("email", ""),
            "city": persona.get("city", ""),
            "switch_action": "manual_ui_persona_selected",
        },
    )
    kafka_producer.send(topic=topic, event=envelope, key=session["user_id"])

    return jsonify({"success": True, "session": session, "event_emitted": envelope})


# -----------------------------------------------------------------------------
# Browsing & Search Events
# -----------------------------------------------------------------------------
@app.route("/api/events/browse", methods=["POST"])
def emit_browse_event():
    """Emit category_browsed or product_searched event from manual UI."""
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    event_type = data.get("event_type", "category_browsed")
    payload = data.get("payload", {})

    envelope, topic = event_builder.build_event(
        event_type=event_type,
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload=payload,
    )
    kafka_producer.send(topic=topic, event=envelope, key=user_id)

    return jsonify({"success": True, "event": envelope, "topic": topic})


@app.route("/api/events/view", methods=["POST"])
def emit_view_event():
    """Emit product_viewed event when user inspects a product card."""
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    product_id = data.get("product_id")
    product = catalog.get_by_id(product_id)
    if not product:
        return jsonify({"success": False, "error": "Product not found"}), 404

    payload = {
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "rating": product["rating"],
        "stock": product["stock"],
    }

    envelope, topic = event_builder.build_event(
        event_type="product_viewed",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload=payload,
    )
    kafka_producer.send(topic=topic, event=envelope, key=user_id)

    return jsonify({"success": True, "event": envelope, "topic": topic, "product": product})


# -----------------------------------------------------------------------------
# Cart Operations & Events
# -----------------------------------------------------------------------------
@app.route("/api/cart", methods=["GET"])
def get_cart():
    """Retrieve current shopping cart for session."""
    session_id = request.args.get("session_id") or request.headers.get("X-Session-ID")
    if not session_id:
        return jsonify({"success": False, "error": "Missing session_id"}), 400

    cart = state_store.get_cart(session_id)
    return jsonify({"success": True, "cart": cart})


@app.route("/api/cart/add", methods=["POST"])
def add_to_cart():
    """Add item to cart and emit cart_item_added event."""
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    product_id = data.get("product_id")
    quantity = int(data.get("quantity", 1))

    product = catalog.get_by_id(product_id)
    if not product:
        return jsonify({"success": False, "error": f"Product '{product_id}' not found"}), 404

    cart = state_store.add_to_cart(session_id, product, quantity=quantity)

    payload = {
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity_added": quantity,
        "cart_subtotal": cart["subtotal"],
        "cart_item_count": cart["item_count"],
    }

    envelope, topic = event_builder.build_event(
        event_type="cart_item_added",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload=payload,
    )
    kafka_producer.send(topic=topic, event=envelope, key=user_id)

    return jsonify({"success": True, "cart": cart, "event": envelope, "topic": topic})


@app.route("/api/cart/remove", methods=["POST"])
def remove_from_cart():
    """Remove item or decrement quantity and emit cart_item_removed event."""
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    product_id = data.get("product_id")
    quantity = data.get("quantity")  # None means remove entire line item

    cart = state_store.remove_from_cart(session_id, product_id, quantity=quantity)

    payload = {
        "product_id": product_id,
        "quantity_removed": quantity or "all",
        "cart_subtotal": cart["subtotal"],
        "cart_item_count": cart["item_count"],
    }

    envelope, topic = event_builder.build_event(
        event_type="cart_item_removed",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload=payload,
    )
    kafka_producer.send(topic=topic, event=envelope, key=user_id)

    return jsonify({"success": True, "cart": cart, "event": envelope, "topic": topic})


@app.route("/api/cart/clear", methods=["POST"])
def clear_cart():
    """Clear all items from cart and emit cart_cleared event."""
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    cart = state_store.clear_cart(session_id)

    envelope, topic = event_builder.build_event(
        event_type="cart_cleared",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload={"reason": data.get("reason", "manual_user_action")},
    )
    kafka_producer.send(topic=topic, event=envelope, key=user_id)

    return jsonify({"success": True, "cart": cart, "event": envelope, "topic": topic})


# -----------------------------------------------------------------------------
# Checkout & Order Placement Endpoints
# -----------------------------------------------------------------------------
@app.route("/api/checkout/start", methods=["POST"])
def start_checkout():
    """Initiate checkout flow and emit checkout_started event."""
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    cart = state_store.get_cart(session_id)
    if not cart["items"]:
        return jsonify({"success": False, "error": "Cannot checkout with an empty cart"}), 400

    payload = {
        "cart_subtotal": cart["subtotal"],
        "item_count": cart["item_count"],
        "items": [
            {"product_id": i["product_id"], "name": i["name"], "price": i["price"], "qty": i["quantity"]}
            for i in cart["items"]
        ],
    }

    envelope, topic = event_builder.build_event(
        event_type="checkout_started",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload=payload,
    )
    kafka_producer.send(topic=topic, event=envelope, key=user_id)

    return jsonify({"success": True, "event": envelope, "topic": topic, "cart": cart})


@app.route("/api/checkout/complete", methods=["POST"])
def complete_checkout():
    """
    Finalize checkout, emit payment_processed and order_placed events,
    and persist order in state store.
    """
    data = request.get_json(force=True)
    session_id, user_id, device_type = _get_request_metadata(data)

    shipping_info = data.get("shipping", {
        "address": "456 Mission St",
        "city": "San Francisco",
        "state": "CA",
        "zip": "94105",
    })
    payment_info = data.get("payment", {
        "method": "credit_card",
        "last4": "4242",
    })

    try:
        # Create order in state store (clears cart)
        order = state_store.create_order(
            session_id=session_id,
            user_id=user_id,
            shipping_info=shipping_info,
            payment_info=payment_info,
        )
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400

    # 1. Emit payment_processed event
    pay_envelope, pay_topic = event_builder.build_event(
        event_type="payment_processed",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload={
            "order_id": order["order_id"],
            "amount": order["total"],
            "currency": "USD",
            "payment_method": payment_info.get("method", "credit_card"),
            "status": "CAPTURED",
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        },
    )
    kafka_producer.send(topic=pay_topic, event=pay_envelope, key=user_id)

    # 2. Emit order_placed event
    order_envelope, order_topic = event_builder.build_event(
        event_type="order_placed",
        user_id=user_id,
        session_id=session_id,
        device_type=device_type,
        source="manual_ui",
        payload={
            "order_id": order["order_id"],
            "total_amount": order["total"],
            "items_count": len(order["items"]),
            "items": order["items"],
            "shipping_city": shipping_info.get("city", "San Francisco"),
        },
    )
    kafka_producer.send(topic=order_topic, event=order_envelope, key=user_id)

    return jsonify({
        "success": True,
        "order": order,
        "events": {
            "payment_processed": pay_envelope,
            "order_placed": order_envelope,
        },
    })


# -----------------------------------------------------------------------------
# Observability & Event Stream Endpoints
# -----------------------------------------------------------------------------
@app.route("/api/events", methods=["GET"])
def get_events():
    """Poll recent events from the rolling buffer."""
    limit = int(request.args.get("limit", 50))
    since_seq = request.args.get("since_seq")
    if since_seq is not None:
        since_seq = int(since_seq)
    topic = request.args.get("topic")
    source = request.args.get("source")

    events = state_store.get_recent_events(
        limit=limit,
        since_seq=since_seq,
        topic=topic,
        source=source,
    )
    return jsonify({
        "success": True,
        "count": len(events),
        "events": events,
        "latest_seq": state_store.get_stats()["latest_seq"],
    })


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Return consolidated telemetry for Kafka, StateStore, and Simulator."""
    return jsonify({
        "success": True,
        "state_store": state_store.get_stats(),
        "kafka": kafka_producer.get_status(),
        "simulator": simulator.get_status(),
    })


@app.route("/api/state/reset", methods=["POST"])
def reset_state():
    """Reset state store buffer, orders, and carts."""
    state_store.reset()
    return jsonify({"success": True, "message": "State store and event buffer reset successfully."})


# -----------------------------------------------------------------------------
# Traffic Simulator Controls
# -----------------------------------------------------------------------------
@app.route("/api/simulator/status", methods=["GET"])
def simulator_status():
    """Get current simulator status and active bots."""
    return jsonify({"success": True, "simulator": simulator.get_status()})


@app.route("/api/simulator/start", methods=["POST"])
def simulator_start():
    """Start the background traffic simulator."""
    data = request.get_json(silent=True) or {}
    if "num_users" in data:
        simulator.set_num_users(int(data["num_users"]))
    if "speed" in data:
        simulator.set_speed(float(data["speed"]))

    simulator.start()
    return jsonify({"success": True, "simulator": simulator.get_status()})


@app.route("/api/simulator/stop", methods=["POST"])
def simulator_stop():
    """Stop the background traffic simulator."""
    simulator.stop()
    return jsonify({"success": True, "simulator": simulator.get_status()})


@app.route("/api/simulator/config", methods=["POST"])
def simulator_config():
    """Update simulator speed or number of concurrent bots on the fly."""
    data = request.get_json(force=True)
    if "num_users" in data:
        simulator.set_num_users(int(data["num_users"]))
    if "speed" in data:
        simulator.set_speed(float(data["speed"]))

    return jsonify({"success": True, "simulator": simulator.get_status()})


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=config.FLASK_PORT,
        debug=config.FLASK_DEBUG,
    )
