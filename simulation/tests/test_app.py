import json
import threading
import uuid
import pytest

from app import app
from catalog import catalog
from config import config
from event_builder import EventValidationError, event_builder
from kafka_producer import KafkaProducerClient, kafka_producer
from simulator import TrafficSimulator, VirtualShopperBot, simulator
from state_store import StateStore, state_store


# =============================================================================
# 1. Configuration Tests
# =============================================================================
def test_config_defaults_and_safe_dict():
    """Verify default configurations and sensitive data masking."""
    assert config.KAFKA_MODE in ("MOCK", "SASL_SSL", "SSL", "PLAINTEXT")
    assert config.APP_VERSION == "2.4.1"
    assert config.TOPIC_USERS == "ecom.users"
    assert config.TOPIC_BROWSE == "ecom.browse"
    assert config.TOPIC_CART == "ecom.cart"
    assert config.TOPIC_ORDERS == "ecom.orders"

    safe_dict = config.to_safe_dict()
    assert "kafka_mode" in safe_dict
    assert "topics" in safe_dict
    assert "SECRET_KEY" not in safe_dict


# =============================================================================
# 2. Product Catalog & Persona Generator Tests
# =============================================================================
def test_catalog_richness_and_filtering():
    """Ensure catalog has 30+ products across specified categories and filtering works."""
    all_products = catalog.get_all()
    assert len(all_products) >= 30, f"Expected 30+ products, got {len(all_products)}"

    categories = catalog.get_categories()
    expected_categories = {"Electronics", "Apparel", "Home & Kitchen", "Books & Media"}
    assert expected_categories.issubset(set(categories))

    # Category filter
    electronics = catalog.get_all(category="Electronics")
    assert len(electronics) >= 8
    assert all(p["category"] == "Electronics" for p in electronics)

    # Search filter
    search_results = catalog.get_all(search="Headphones")
    assert len(search_results) >= 1
    assert any("Sony" in p["name"] for p in search_results)

    # Get by ID
    p_id = all_products[0]["id"]
    single_p = catalog.get_by_id(p_id)
    assert single_p is not None
    assert single_p["id"] == p_id

    # Simulated persona generation
    persona = catalog.generate_random_user()
    assert "user_id" in persona
    assert "name" in persona
    assert "email" in persona
    assert persona["device_type"] in ("web", "ios", "android")


# =============================================================================
# 3. Event Builder & Schema Validation Tests
# =============================================================================
def test_event_builder_schema_compliance():
    """Verify that emitted events adhere strictly to the exact required envelope."""
    envelope, topic = event_builder.build_event(
        event_type="cart_item_added",
        user_id="usr_test_99",
        session_id="sess_test_99",
        device_type="web",
        source="manual_ui",
        payload={"product_id": "prod-001", "quantity": 2},
    )

    # Check top-level envelope schema
    assert "event_id" in envelope
    assert "event_type" in envelope
    assert "event_timestamp" in envelope
    assert "user_id" in envelope
    assert "session_id" in envelope
    assert "device_type" in envelope
    assert "app_version" in envelope
    assert "source" in envelope
    assert "payload" in envelope

    assert envelope["event_type"] == "cart_item_added"
    assert envelope["user_id"] == "usr_test_99"
    assert envelope["session_id"] == "sess_test_99"
    assert envelope["device_type"] == "web"
    assert envelope["source"] == "manual_ui"
    assert envelope["app_version"] == "2.4.1"
    assert envelope["payload"] == {"product_id": "prod-001", "quantity": 2}
    assert topic == "ecom.cart"


def test_event_builder_strict_validations():
    """Test schema validation rejections for invalid fields or types."""
    # Invalid device_type
    with pytest.raises(EventValidationError, match="Invalid device_type"):
        event_builder.build_event(
            event_type="test",
            user_id="u1",
            session_id="s1",
            device_type="smart_fridge",
            payload={},
        )

    # Invalid source
    with pytest.raises(EventValidationError, match="Invalid source"):
        event_builder.build_event(
            event_type="test",
            user_id="u1",
            session_id="s1",
            source="external_webhook",
            payload={},
        )

    # Non-dict payload
    with pytest.raises(EventValidationError, match="payload must be a dictionary"):
        event_builder.build_event(
            event_type="test",
            user_id="u1",
            session_id="s1",
            payload="invalid_payload_string",  # type: ignore
        )

    # Missing mandatory field in raw envelope validation
    raw_envelope = {
        "event_id": str(uuid.uuid4()),
        "event_type": "test",
        # missing session_id, user_id, etc.
    }
    with pytest.raises(EventValidationError, match="Missing required envelope fields"):
        event_builder.validate_event(raw_envelope)


# =============================================================================
# 4. Kafka Topic Routing Table Tests
# =============================================================================
@pytest.mark.parametrize(
    "event_type,expected_topic",
    [
        ("user_registered", "ecom.users"),
        ("user_login", "ecom.users"),
        ("user_logout", "ecom.users"),
        ("product_viewed", "ecom.browse"),
        ("product_searched", "ecom.browse"),
        ("category_browsed", "ecom.browse"),
        ("cart_item_added", "ecom.cart"),
        ("cart_item_removed", "ecom.cart"),
        ("cart_cleared", "ecom.cart"),
        ("checkout_started", "ecom.orders"),
        ("checkout_step_completed", "ecom.orders"),
        ("order_placed", "ecom.orders"),
        ("payment_processed", "ecom.orders"),
        ("custom_unmapped_event", "ecom.orders"),
    ],
)
def test_topic_routing_map(event_type, expected_topic):
    """Verify that all event types route to their canonical Kafka topic."""
    assert event_builder.get_topic_for_event_type(event_type) == expected_topic


# =============================================================================
# 5. Thread-Safe StateStore & Rolling 100-Event Buffer Tests
# =============================================================================
def test_state_store_cart_lifecycle():
    """Verify shopping cart state operations: add, increment, decrement, and clear."""
    store = StateStore(max_event_buffer=100)
    sid = "sess_cart_test"

    product1 = {"id": "p1", "name": "Item 1", "price": 50.0, "category": "Test"}
    product2 = {"id": "p2", "name": "Item 2", "price": 30.0, "category": "Test"}

    # Add item 1
    cart = store.add_to_cart(sid, product1, quantity=1)
    assert cart["item_count"] == 1
    assert cart["subtotal"] == 50.0

    # Increment item 1
    cart = store.add_to_cart(sid, product1, quantity=2)
    assert cart["item_count"] == 3
    assert cart["subtotal"] == 150.0

    # Add item 2
    cart = store.add_to_cart(sid, product2, quantity=1)
    assert cart["item_count"] == 4
    assert cart["subtotal"] == 180.0

    # Decrement item 1 by 1
    cart = store.remove_from_cart(sid, "p1", quantity=1)
    assert cart["item_count"] == 3
    assert cart["subtotal"] == 130.0

    # Remove item 2 entirely
    cart = store.remove_from_cart(sid, "p2", quantity=None)
    assert cart["item_count"] == 2
    assert cart["subtotal"] == 100.0

    # Clear cart
    cart = store.clear_cart(sid)
    assert cart["item_count"] == 0
    assert cart["subtotal"] == 0.0
    assert len(cart["items"]) == 0


def test_state_store_order_placement():
    """Verify creating orders calculates taxes, shipping, clears cart, and stores order."""
    store = StateStore(max_event_buffer=100)
    sid = "sess_order_test"
    uid = "usr_order_test"

    # Add items to cart
    store.add_to_cart(sid, {"id": "p1", "name": "Monitor", "price": 200.0}, quantity=1)

    order = store.create_order(
        session_id=sid,
        user_id=uid,
        shipping_info={"city": "Seattle", "state": "WA"},
        payment_info={"method": "credit_card", "last4": "1111"},
    )

    assert order["order_id"].startswith("ord_")
    assert order["subtotal"] == 200.0
    assert order["tax"] == 16.0  # 8% of 200
    assert order["shipping_fee"] == 0.0  # over $100 free
    assert order["total"] == 216.0
    assert order["status"] == "CONFIRMED"

    # Verify cart is now empty
    empty_cart = store.get_cart(sid)
    assert empty_cart["item_count"] == 0

    # Cannot create order with empty cart
    with pytest.raises(ValueError, match="empty cart"):
        store.create_order(sid, uid)


def test_state_store_rolling_100_event_buffer():
    """Verify that the rolling event buffer caps strictly at 100 events and drops oldest."""
    store = StateStore(max_event_buffer=100)

    # Emit 125 events
    for i in range(1, 126):
        envelope = {
            "event_id": f"evt_{i}",
            "event_type": "product_viewed",
            "event_timestamp": "2026-11-23T12:00:00Z",
            "user_id": f"u_{i}",
            "session_id": f"s_{i}",
            "device_type": "web",
            "app_version": "2.4.1",
            "source": "auto_simulator",
            "payload": {"idx": i},
        }
        store.record_event(envelope, topic="ecom.browse")

    stats = store.get_stats()
    assert stats["total_events_emitted"] == 125
    assert stats["current_buffer_size"] == 100  # capped at 100
    assert stats["latest_seq"] == 125

    recent_events = store.get_recent_events(limit=100)
    assert len(recent_events) == 100
    # Oldest in buffer should be event #26
    assert recent_events[0]["seq"] == 26
    # Newest in buffer should be event #125
    assert recent_events[-1]["seq"] == 125

    # Test since_seq filter
    filtered = store.get_recent_events(since_seq=120)
    assert len(filtered) == 5
    assert [e["seq"] for e in filtered] == [121, 122, 123, 124, 125]


def test_state_store_concurrent_thread_safety():
    """Verify thread-safety of state store under concurrent reads/writes."""
    store = StateStore(max_event_buffer=100)
    errors = []

    def worker(worker_id):
        try:
            sid = f"sess_thread_{worker_id}"
            prod = {"id": f"p_{worker_id}", "name": f"P {worker_id}", "price": 10.0}
            for _ in range(20):
                store.add_to_cart(sid, prod, quantity=1)
                envelope = {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "cart_item_added",
                    "event_timestamp": "2026-11-23T12:00:00Z",
                    "user_id": f"u_{worker_id}",
                    "session_id": sid,
                    "device_type": "web",
                    "app_version": "2.4.1",
                    "source": "manual_ui",
                    "payload": {},
                }
                store.record_event(envelope, topic="ecom.cart")
                store.get_stats()
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Thread errors encountered: {errors}"
    stats = store.get_stats()
    assert stats["total_events_emitted"] == 200
    assert stats["current_buffer_size"] == 100


# =============================================================================
# 6. Modular Kafka Producer (Mock Mode) Tests
# =============================================================================
def test_kafka_producer_mock_mode():
    """Test modular Kafka producer operates reliably in MOCK mode without active broker."""
    producer = KafkaProducerClient(mode="MOCK")
    status = producer.get_status()
    assert status["is_mock"] is True
    assert status["mode"] == "MOCK"

    envelope = {
        "event_id": str(uuid.uuid4()),
        "event_type": "user_login",
        "event_timestamp": "2026-11-23T12:00:00Z",
        "user_id": "usr_mock_1",
        "session_id": "sess_mock_1",
        "device_type": "web",
        "app_version": "2.4.1",
        "source": "manual_ui",
        "payload": {},
    }

    delivery_results = []

    def mock_cb(err, msg):
        delivery_results.append((err, msg))

    success = producer.send(
        topic="ecom.users",
        event=envelope,
        key="usr_mock_1",
        callback=mock_cb,
    )

    assert success is True
    assert len(delivery_results) == 1
    assert delivery_results[0][0] is None  # no error
    assert "MOCK_DELIVERED" in delivery_results[0][1]

    updated_status = producer.get_status()
    assert updated_status["total_sent"] >= 1
    assert updated_status["mock_sent"] >= 1
    assert updated_status["total_failed"] == 0


# =============================================================================
# 7. Traffic Simulator & Markov State Transitions Tests
# =============================================================================
def test_traffic_simulator_lifecycle_and_tick():
    """Verify traffic simulator step execution, Markov state transitions, and event emission."""
    sim = TrafficSimulator()
    sim.set_num_users(3)
    assert len(sim._bots) == 3

    # Initial state should be IDLE
    assert sim._bots[0].state == "IDLE"

    # Execute 1 tick: advances bot 0 from IDLE -> user_login
    event1 = sim.tick()
    assert event1 is not None
    assert event1["event_type"] == "user_login"
    assert event1["source"] == "auto_simulator"
    assert sim._bots[0].state in ("BROWSING", "SEARCHING")

    # Execute multiple ticks and verify diverse event types emitted
    emitted_types = {event1["event_type"]}
    for _ in range(15):
        evt = sim.tick()
        if evt:
            emitted_types.add(evt["event_type"])

    assert len(emitted_types) > 1, f"Expected varied events from Markov transitions, got {emitted_types}"

    # Verify start and stop
    assert not sim.is_running()
    sim.start()
    assert sim.is_running()
    sim.stop()
    assert not sim.is_running()


# =============================================================================
# 8. Flask REST API Integration Endpoints Tests
# =============================================================================
@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        # Reset state before tests
        state_store.reset()
        yield client


def test_api_catalog_endpoints(client):
    """Test catalog retrieval and detail endpoints."""
    # List all
    res = client.get("/api/catalog")
    assert res.status_code == 200
    data = res.get_json()
    assert data["success"] is True
    assert data["count"] >= 30

    # Filter category
    res_cat = client.get("/api/catalog?category=Electronics")
    data_cat = res_cat.get_json()
    assert data_cat["success"] is True
    assert all(p["category"] == "Electronics" for p in data_cat["products"])

    # Detail
    p_id = data["products"][0]["id"]
    res_detail = client.get(f"/api/catalog/{p_id}")
    assert res_detail.status_code == 200
    assert res_detail.get_json()["product"]["id"] == p_id

    # Non-existent
    res_404 = client.get("/api/catalog/non-existent-product")
    assert res_404.status_code == 404


def test_api_cart_flow_and_event_emission(client):
    """Test adding items, viewing cart, and removing items via REST API."""
    session_id = "test_flask_session_1"
    user_id = "test_flask_user_1"

    # Add item
    add_payload = {
        "session_id": session_id,
        "user_id": user_id,
        "device_type": "web",
        "product_id": "prod-el-001",
        "quantity": 2,
    }
    res_add = client.post("/api/cart/add", json=add_payload)
    assert res_add.status_code == 200
    data_add = res_add.get_json()
    assert data_add["success"] is True
    assert data_add["cart"]["item_count"] == 2
    assert data_add["event"]["event_type"] == "cart_item_added"
    assert data_add["topic"] == "ecom.cart"

    # Get cart
    res_get = client.get(f"/api/cart?session_id={session_id}")
    assert res_get.status_code == 200
    data_get = res_get.get_json()
    assert data_get["cart"]["item_count"] == 2

    # Remove item
    remove_payload = {
        "session_id": session_id,
        "user_id": user_id,
        "device_type": "web",
        "product_id": "prod-el-001",
        "quantity": 1,
    }
    res_rem = client.post("/api/cart/remove", json=remove_payload)
    assert res_rem.status_code == 200
    assert res_rem.get_json()["cart"]["item_count"] == 1


def test_api_checkout_and_order_flow(client):
    """Test full checkout flow: start, complete order, verify emitted events and store state."""
    session_id = "test_flask_checkout_session"
    user_id = "test_flask_checkout_user"

    # Add product
    client.post(
        "/api/cart/add",
        json={
            "session_id": session_id,
            "user_id": user_id,
            "device_type": "ios",
            "product_id": "prod-bk-001",
            "quantity": 1,
        },
    )

    # Start checkout
    res_start = client.post(
        "/api/checkout/start",
        json={"session_id": session_id, "user_id": user_id, "device_type": "ios"},
    )
    assert res_start.status_code == 200
    assert res_start.get_json()["event"]["event_type"] == "checkout_started"

    # Complete checkout
    complete_payload = {
        "session_id": session_id,
        "user_id": user_id,
        "device_type": "ios",
        "shipping": {"address": "123 Tech Way", "city": "Austin", "state": "TX", "zip": "78701"},
        "payment": {"method": "apple_pay", "last4": "8888"},
    }
    res_complete = client.post("/api/checkout/complete", json=complete_payload)
    assert res_complete.status_code == 200
    data_complete = res_complete.get_json()
    assert data_complete["success"] is True
    assert data_complete["order"]["order_id"].startswith("ord_")
    assert "payment_processed" in data_complete["events"]
    assert "order_placed" in data_complete["events"]

    # Verify recent events endpoint includes order_placed
    res_events = client.get("/api/events?topic=ecom.orders")
    assert res_events.status_code == 200
    events_data = res_events.get_json()
    assert any(e["event"]["event_type"] == "order_placed" for e in events_data["events"])


def test_api_simulator_endpoints(client):
    """Test simulator control routes."""
    # Status
    res_status = client.get("/api/simulator/status")
    assert res_status.status_code == 200
    assert "simulator" in res_status.get_json()

    # Start
    res_start = client.post("/api/simulator/start", json={"num_users": 3, "speed": 1.0})
    assert res_start.status_code == 200
    assert res_start.get_json()["simulator"]["is_running"] is True

    # Stop
    res_stop = client.post("/api/simulator/stop")
    assert res_stop.status_code == 200
    assert res_stop.get_json()["simulator"]["is_running"] is False
