#!/usr/bin/env python3
"""
Generate high-fidelity bulk pre-load data for modern Lakehouse / Databricks data engineering.
Generates:
1. data/bulk_transactions_preload.json (NDJSON) — Order lifecycle transactions with CDC operations & anomalies
2. data/customer_profiles_scd.json (NDJSON) — Customer profiles with SCD Type 1 & Type 2 historical versions
"""

import copy
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone

# Add simulation folder to path to reuse catalog items & personas
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "simulation"))
from catalog import CATALOG_ITEMS, SIMULATED_PERSONAS

random.seed(42)

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "bulk_transactions_preload.json")
CUSTOMERS_FILE = os.path.join(DATA_DIR, "customer_profiles_scd.json")

# Base timestamp anchors (simulating historical data from past 30 days)
BASE_TIME = datetime(2026, 11, 1, 8, 0, 0, tzinfo=timezone.utc)


def generate_customer_profiles_scd():
    """
    Generate customer master records demonstrating SCD Type 1 and SCD Type 2 changes:
    - Version 1: Initial signup state
    - Version 2: Location change or loyalty tier upgrade (SCD 2)
    - Version 3 / updates: Typo fixes (SCD 1)
    """
    profiles = []

    # 1. Elena Rostova - Relocated and Upgraded Tier (SCD Type 2)
    profiles.append({
        "customer_id": "usr_alpha_101",
        "name": "Elena Rostova",
        "email": "elena.rostova@example.com",
        "phone": "+1-206-555-0142",
        "street": "400 Pine St, Apt 12B",
        "city": "Seattle",
        "state": "WA",
        "zip": "98101",
        "country": "US",
        "customer_tier": "SILVER",
        "preferred_category": "Electronics",
        "lifetime_spend": 1420.50,
        "is_active": True,
        "version": 1,
        "effective_from": "2026-01-15T09:00:00Z",
        "effective_to": "2026-08-20T14:30:00Z",
        "is_current": False,
        "scd_change_reason": "INITIAL_ONBOARDING",
    })
    profiles.append({
        "customer_id": "usr_alpha_101",
        "name": "Elena Rostova",
        "email": "elena.rostova@example.com",
        "phone": "+1-415-555-0189",  # Phone updated
        "street": "750 Montgomery St",
        "city": "San Francisco",  # Moved from Seattle -> SF (SCD 2)
        "state": "CA",
        "zip": "94111",
        "country": "US",
        "customer_tier": "GOLD",  # Upgraded Tier (SCD 2)
        "preferred_category": "Electronics",
        "lifetime_spend": 3890.00,
        "is_active": True,
        "version": 2,
        "effective_from": "2026-08-20T14:30:00Z",
        "effective_to": "9999-12-31T23:59:59Z",
        "is_current": True,
        "scd_change_reason": "RELOCATION_AND_TIER_PROMOTION",
    })

    # 2. Marcus Chen - SCD Type 2 Tier Promotion (Austin, TX)
    profiles.append({
        "customer_id": "usr_beta_202",
        "name": "Marcus Chen",
        "email": "marcus.chen@example.com",
        "phone": "+1-512-555-0177",
        "street": "1200 Congress Ave",
        "city": "Austin",
        "state": "TX",
        "zip": "78701",
        "country": "US",
        "customer_tier": "BRONZE",
        "preferred_category": "Books & Media",
        "lifetime_spend": 340.00,
        "is_active": True,
        "version": 1,
        "effective_from": "2026-02-10T11:15:00Z",
        "effective_to": "2026-09-01T10:00:00Z",
        "is_current": False,
        "scd_change_reason": "INITIAL_ONBOARDING",
    })
    profiles.append({
        "customer_id": "usr_beta_202",
        "name": "Marcus Chen",
        "email": "marcus.chen@example.com",
        "phone": "+1-512-555-0177",
        "street": "1200 Congress Ave",
        "city": "Austin",
        "state": "TX",
        "zip": "78701",
        "country": "US",
        "customer_tier": "PLATINUM",  # Big VIP Upgrade
        "preferred_category": "Books & Media",
        "lifetime_spend": 2150.00,
        "is_active": True,
        "version": 2,
        "effective_from": "2026-09-01T10:00:00Z",
        "effective_to": "9999-12-31T23:59:59Z",
        "is_current": True,
        "scd_change_reason": "VIP_SPEND_THRESHOLD_REACHED",
    })

    # 3. Amina Diallo - SCD Type 1 In-Place Correction (Phone & Street Typo Fix)
    profiles.append({
        "customer_id": "usr_gamma_303",
        "name": "Amina Diallo",
        "email": "amina.diallo@example.com",
        "phone": "+1-212-555-0199",
        "street": "55 5th Ave, Floor 8",
        "city": "New York",
        "state": "NY",
        "zip": "10003",
        "country": "US",
        "customer_tier": "GOLD",
        "preferred_category": "Apparel",
        "lifetime_spend": 1820.00,
        "is_active": True,
        "version": 1,
        "effective_from": "2026-03-01T08:00:00Z",
        "effective_to": "9999-12-31T23:59:59Z",
        "is_current": True,
        "scd_change_reason": "SCD1_CONTACT_CORRECTION",
    })

    # 4. Liam Gallagher - Home & Kitchen Persona (Denver, CO)
    profiles.append({
        "customer_id": "usr_delta_404",
        "name": "Liam Gallagher",
        "email": "liam.g@example.com",
        "phone": "+1-303-555-0131",
        "street": "1600 California St",
        "city": "Denver",
        "state": "CO",
        "zip": "80202",
        "country": "US",
        "customer_tier": "SILVER",
        "preferred_category": "Home & Kitchen",
        "lifetime_spend": 980.00,
        "is_active": True,
        "version": 1,
        "effective_from": "2026-04-12T13:45:00Z",
        "effective_to": "9999-12-31T23:59:59Z",
        "is_current": True,
        "scd_change_reason": "INITIAL_ONBOARDING",
    })

    # 5. Sophia Morales - Tech Persona (San Francisco, CA)
    profiles.append({
        "customer_id": "usr_epsilon_505",
        "name": "Sophia Morales",
        "email": "sophia.m@example.com",
        "phone": "+1-415-555-0112",
        "street": "300 Brannan St",
        "city": "San Francisco",
        "state": "CA",
        "zip": "94107",
        "country": "US",
        "customer_tier": "PLATINUM",
        "preferred_category": "Electronics",
        "lifetime_spend": 4500.00,
        "is_active": True,
        "version": 1,
        "effective_from": "2026-02-28T16:20:00Z",
        "effective_to": "9999-12-31T23:59:59Z",
        "is_current": True,
        "scd_change_reason": "INITIAL_ONBOARDING",
    })

    # Additional 15 synthetic user profiles for scale
    cities = [
        ("Chicago", "IL", "60601"),
        ("Boston", "MA", "02108"),
        ("Miami", "FL", "33101"),
        ("Seattle", "WA", "98104"),
        ("Atlanta", "GA", "30303"),
    ]
    tiers = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Books & Media"]

    for i in range(6, 21):
        uid = f"usr_syn_{100 + i}"
        city_info = cities[i % len(cities)]
        profiles.append({
            "customer_id": uid,
            "name": f"Customer {i}",
            "email": f"customer.{i}@sample-store.com",
            "phone": f"+1-555-01{i:02d}",
            "street": f"{100 * i} Main St",
            "city": city_info[0],
            "state": city_info[1],
            "zip": city_info[2],
            "country": "US",
            "customer_tier": tiers[i % len(tiers)],
            "preferred_category": categories[i % len(categories)],
            "lifetime_spend": round(random.uniform(150.0, 3200.0), 2),
            "is_active": True,
            "version": 1,
            "effective_from": "2026-05-01T00:00:00Z",
            "effective_to": "9999-12-31T23:59:59Z",
            "is_current": True,
            "scd_change_reason": "INITIAL_ONBOARDING",
        })

    return profiles


def generate_transactions_with_cdc_and_anomalies(num_base_orders: int = 500):
    """
    Generate rich order transactions containing:
    1. Standard clean orders (INSERT)
    2. Multi-step CDC lifecycle evolutions (INSERT -> UPDATE status -> DELIVERED / CANCELLED)
    3. Real-world Data Engineering Anomalies:
       - Late-Arriving Data (out-of-order timestamps)
       - Duplicate Event/Transaction IDs (idempotency tests)
       - Null / Missing mandatory fields (data quality expectations)
       - Outlier / Negative values (data cleansing / quarantine)
    """
    all_users = [p["user_id"] for p in SIMULATED_PERSONAS] + [f"usr_syn_{100 + i}" for i in range(6, 21)]
    records = []
    current_time = BASE_TIME
    rec_counter = 1

    # Pre-select order IDs for CDC state evolution
    cdc_lifecycle_orders = [f"ord_cdc_{1000 + i}" for i in range(50)]

    for i in range(num_base_orders):
        order_id = f"ord_hist_{10000 + i}"
        user_id = random.choice(all_users)
        session_id = f"sess_{uuid.uuid4().hex[:10]}"
        device = random.choices(["web", "ios", "android"], weights=[0.55, 0.30, 0.15])[0]

        # Time progression across 20 days
        current_time += timedelta(minutes=random.randint(5, 45))
        created_at_iso = current_time.isoformat()

        # Select 1 to 4 items from catalog
        num_items = random.choices([1, 2, 3, 4], weights=[0.6, 0.25, 0.1, 0.05])[0]
        selected_prods = random.sample(CATALOG_ITEMS, num_items)

        items_payload = []
        subtotal = 0.0
        for p in selected_prods:
            qty = random.randint(1, 2)
            line_tot = round(p["price"] * qty, 2)
            subtotal += line_tot
            items_payload.append({
                "product_id": p["id"],
                "product_name": p["name"],
                "category": p["category"],
                "unit_price": p["price"],
                "quantity": qty,
                "total_price": line_tot,
            })

        subtotal = round(subtotal, 2)
        tax = round(subtotal * 0.08, 2)
        shipping = 0.0 if subtotal >= 100.0 else 9.99
        total = round(subtotal + tax + shipping, 2)

        record = {
            "record_id": f"rec_{rec_counter:06d}",
            "order_id": order_id,
            "user_id": user_id,
            "session_id": session_id,
            "transaction_timestamp": created_at_iso,
            "updated_at": created_at_iso,
            "cdc_operation": "INSERT",
            "order_status": "CONFIRMED",
            "currency": "USD",
            "subtotal": subtotal,
            "tax": tax,
            "shipping_fee": shipping,
            "total_amount": total,
            "discount_amount": 0.0,
            "payment_method": random.choice(["credit_card", "apple_pay", "google_pay", "paypal"]),
            "payment_status": "CAPTURED",
            "shipping_city": random.choice(["San Francisco", "Austin", "New York", "Seattle", "Denver"]),
            "shipping_state": random.choice(["CA", "TX", "NY", "WA", "CO"]),
            "shipping_country": "US",
            "device_type": device,
            "items_count": len(items_payload),
            "items": items_payload,
            "data_quality_flags": ["CLEAN"],
        }
        records.append(record)
        rec_counter += 1

    # -------------------------------------------------------------------------
    # Injected Scenario 1: Multi-Step CDC State Evolutions (Status Mutations)
    # Models orders progressing: PENDING -> CONFIRMED -> SHIPPED -> DELIVERED / CANCELLED
    # -------------------------------------------------------------------------
    for cdc_ord_id in cdc_lifecycle_orders:
        uid = random.choice(all_users)
        t0 = current_time + timedelta(hours=random.randint(1, 10))
        prod = random.choice(CATALOG_ITEMS)
        tot = prod["price"]

        base_rec = {
            "record_id": f"rec_{rec_counter:06d}",
            "order_id": cdc_ord_id,
            "user_id": uid,
            "session_id": f"sess_cdc_{uuid.uuid4().hex[:8]}",
            "transaction_timestamp": t0.isoformat(),
            "updated_at": t0.isoformat(),
            "cdc_operation": "INSERT",
            "order_status": "PENDING",
            "currency": "USD",
            "subtotal": tot,
            "tax": round(tot * 0.08, 2),
            "shipping_fee": 0.0,
            "total_amount": round(tot * 1.08, 2),
            "discount_amount": 0.0,
            "payment_method": "credit_card",
            "payment_status": "PENDING",
            "shipping_city": "Austin",
            "shipping_state": "TX",
            "shipping_country": "US",
            "device_type": "web",
            "items_count": 1,
            "items": [{
                "product_id": prod["id"],
                "product_name": prod["name"],
                "category": prod["category"],
                "unit_price": tot,
                "quantity": 1,
                "total_price": tot,
            }],
            "data_quality_flags": ["CDC_STAGE_1_PENDING"],
        }
        records.append(base_rec)
        rec_counter += 1

        # Step 2: Payment captured -> CONFIRMED
        t1 = t0 + timedelta(minutes=15)
        rec_confirmed = copy.deepcopy(base_rec)
        rec_confirmed["record_id"] = f"rec_{rec_counter:06d}"
        rec_confirmed["updated_at"] = t1.isoformat()
        rec_confirmed["cdc_operation"] = "UPDATE"
        rec_confirmed["order_status"] = "CONFIRMED"
        rec_confirmed["payment_status"] = "CAPTURED"
        rec_confirmed["data_quality_flags"] = ["CDC_STAGE_2_CONFIRMED"]
        records.append(rec_confirmed)
        rec_counter += 1

        # Step 3: 80% SHIPPED, 20% CANCELLED
        t2 = t1 + timedelta(hours=random.randint(4, 24))
        rec_status3 = copy.deepcopy(rec_confirmed)
        rec_status3["record_id"] = f"rec_{rec_counter:06d}"
        rec_status3["updated_at"] = t2.isoformat()
        rec_status3["cdc_operation"] = "UPDATE"

        if random.random() < 0.8:
            rec_status3["order_status"] = "SHIPPED"
            rec_status3["tracking_number"] = f"TRK_{random.randint(10000000, 99999999)}"
            rec_status3["data_quality_flags"] = ["CDC_STAGE_3_SHIPPED"]
        else:
            rec_status3["order_status"] = "CANCELLED"
            rec_status3["payment_status"] = "REFUNDED"
            rec_status3["data_quality_flags"] = ["CDC_STAGE_3_CANCELLED_REFUND"]

        records.append(rec_status3)
        rec_counter += 1

    # -------------------------------------------------------------------------
    # Injected Scenario 2: Late-Arriving Data (Out-of-Order Watermarking Anomaly)
    # Transactions timestamped 3 to 7 days in the past arriving in the current load
    # -------------------------------------------------------------------------
    for k in range(15):
        late_time = current_time - timedelta(days=random.randint(3, 7))
        records.append({
            "record_id": f"rec_{rec_counter:06d}",
            "order_id": f"ord_late_{200 + k}",
            "user_id": random.choice(all_users),
            "session_id": f"sess_late_{k}",
            "transaction_timestamp": late_time.isoformat(),
            "updated_at": current_time.isoformat(),  # Ingested now, but occurred days ago!
            "cdc_operation": "INSERT",
            "order_status": "CONFIRMED",
            "currency": "USD",
            "subtotal": 199.00,
            "tax": 15.92,
            "shipping_fee": 0.0,
            "total_amount": 214.92,
            "discount_amount": 0.0,
            "payment_method": "apple_pay",
            "payment_status": "CAPTURED",
            "shipping_city": "Seattle",
            "shipping_state": "WA",
            "shipping_country": "US",
            "device_type": "ios",
            "items_count": 1,
            "items": [{
                "product_id": "prod-el-003",
                "product_name": "Keychron Q1 Pro Mechanical Keyboard",
                "category": "Electronics",
                "unit_price": 199.00,
                "quantity": 1,
                "total_price": 199.00,
            }],
            "data_quality_flags": ["ANOMALY_LATE_ARRIVING_EVENT"],
        })
        rec_counter += 1

    # -------------------------------------------------------------------------
    # Injected Scenario 3: Duplicate Transaction / Event IDs (Idempotency Anomaly)
    # Exactly identical records re-emitted, challenging Bronze-to-Silver deduplication
    # -------------------------------------------------------------------------
    for dup_idx in [10, 25, 45, 80, 110]:
        target_rec = records[dup_idx]
        dup_rec = copy.deepcopy(target_rec)
        dup_rec["record_id"] = f"rec_{rec_counter:06d}"  # New ingestion ID, same order_id
        dup_rec["data_quality_flags"] = ["ANOMALY_EXACT_DUPLICATE_ORDER_ID"]
        records.append(dup_rec)
        rec_counter += 1

    # -------------------------------------------------------------------------
    # Injected Scenario 4: Missing & Null Critical Fields (Data Quality Anomaly)
    # Missing shipping city, null device_type, or blank payment method
    # -------------------------------------------------------------------------
    records.append({
        "record_id": f"rec_{rec_counter:06d}",
        "order_id": "ord_anomaly_null_city",
        "user_id": "usr_alpha_101",
        "session_id": "sess_bad_addr",
        "transaction_timestamp": current_time.isoformat(),
        "updated_at": current_time.isoformat(),
        "cdc_operation": "INSERT",
        "order_status": "CONFIRMED",
        "currency": "USD",
        "subtotal": 159.00,
        "tax": 12.72,
        "shipping_fee": 0.0,
        "total_amount": 171.72,
        "discount_amount": 0.0,
        "payment_method": "credit_card",
        "payment_status": "CAPTURED",
        "shipping_city": None,  # NULL CITY VIOLATION
        "shipping_state": None,
        "shipping_country": "US",
        "device_type": "web",
        "items_count": 1,
        "items": [],
        "data_quality_flags": ["ANOMALY_NULL_SHIPPING_FIELDS"],
    })
    rec_counter += 1

    records.append({
        "record_id": f"rec_{rec_counter:06d}",
        "order_id": "ord_anomaly_null_device",
        "user_id": "usr_beta_202",
        "session_id": "sess_bad_dev",
        "transaction_timestamp": current_time.isoformat(),
        "updated_at": current_time.isoformat(),
        "cdc_operation": "INSERT",
        "order_status": "CONFIRMED",
        "currency": "USD",
        "subtotal": 49.99,
        "tax": 4.00,
        "shipping_fee": 9.99,
        "total_amount": 63.98,
        "discount_amount": 0.0,
        "payment_method": None,  # NULL PAYMENT METHOD
        "payment_status": "UNKNOWN",
        "shipping_city": "Austin",
        "shipping_state": "TX",
        "shipping_country": "US",
        "device_type": None,  # NULL DEVICE TYPE
        "items_count": 1,
        "items": [],
        "data_quality_flags": ["ANOMALY_NULL_DEVICE_AND_PAYMENT"],
    })
    rec_counter += 1

    # -------------------------------------------------------------------------
    # Injected Scenario 5: Numerical Outlier / Negative Discount (Quarantine Anomaly)
    # Erroneous negative price or discount resulting in invalid totals
    # -------------------------------------------------------------------------
    records.append({
        "record_id": f"rec_{rec_counter:06d}",
        "order_id": "ord_anomaly_neg_amount",
        "user_id": "usr_gamma_303",
        "session_id": "sess_bug_calc",
        "transaction_timestamp": current_time.isoformat(),
        "updated_at": current_time.isoformat(),
        "cdc_operation": "INSERT",
        "order_status": "PENDING",
        "currency": "USD",
        "subtotal": 100.00,
        "tax": 8.00,
        "shipping_fee": 0.0,
        "total_amount": -450.00,  # IMPOSSIBLE NEGATIVE TOTAL AMOUNT
        "discount_amount": 558.00,  # Rogue coupon glitch
        "payment_method": "credit_card",
        "payment_status": "FAILED",
        "shipping_city": "New York",
        "shipping_state": "NY",
        "shipping_country": "US",
        "device_type": "android",
        "items_count": 1,
        "items": [{
            "product_id": "prod-ap-004",
            "product_name": "Lululemon Align High-Rise Pant",
            "category": "Apparel",
            "unit_price": 98.00,
            "quantity": 1,
            "total_price": 98.00,
        }],
        "data_quality_flags": ["ANOMALY_NEGATIVE_TOTAL_AMOUNT"],
    })
    rec_counter += 1

    records.append({
        "record_id": f"rec_{rec_counter:06d}",
        "order_id": "ord_anomaly_qty_outlier",
        "user_id": "usr_delta_404",
        "session_id": "sess_bot_scraping",
        "transaction_timestamp": current_time.isoformat(),
        "updated_at": current_time.isoformat(),
        "cdc_operation": "INSERT",
        "order_status": "PENDING",
        "currency": "USD",
        "subtotal": 249000.00,
        "tax": 19920.00,
        "shipping_fee": 0.0,
        "total_amount": 268920.00,
        "discount_amount": 0.0,
        "payment_method": "credit_card",
        "payment_status": "BLOCKED",
        "shipping_city": "Denver",
        "shipping_state": "CO",
        "shipping_country": "US",
        "device_type": "web",
        "items_count": 1,
        "items": [{
            "product_id": "prod-hk-002",
            "product_name": "Lodge Cast Iron Skillet",
            "category": "Home & Kitchen",
            "unit_price": 24.90,
            "quantity": 10000,  # EXTREME OUTLIER QUANTITY (10,000 cast iron skillets)
            "total_price": 249000.00,
        }],
        "data_quality_flags": ["ANOMALY_EXTREME_QUANTITY_OUTLIER"],
    })
    rec_counter += 1

    # Sort primarily by updated_at to simulate realistic ingestion timeline
    records.sort(key=lambda r: r["updated_at"])
    return records


def main():
    print("Generating customer master profiles (SCD Type 1 & 2)...")
    customers = generate_customer_profiles_scd()
    with open(CUSTOMERS_FILE, "w", encoding="utf-8") as f:
        for c in customers:
            f.write(json.dumps(c) + "\n")
    print(f" Saved {len(customers)} customer profile records -> {CUSTOMERS_FILE}")

    print("Generating bulk transaction pre-load with CDC mutations & anomalies...")
    transactions = generate_transactions_with_cdc_and_anomalies(num_base_orders=650)
    with open(TRANSACTIONS_FILE, "w", encoding="utf-8") as f:
        for t in transactions:
            f.write(json.dumps(t) + "\n")
    print(f" Saved {len(transactions)} transaction pre-load records -> {TRANSACTIONS_FILE}")

    # Also output summary stats
    anomalies = [t for t in transactions if any(f.startswith("ANOMALY") for f in t["data_quality_flags"])]
    cdc_records = [t for t in transactions if any(f.startswith("CDC") for f in t["data_quality_flags"])]
    print(f"\nSummary of Pre-load Dataset:")
    print(f" - Total Transactions: {len(transactions)}")
    print(f" - CDC State Mutation Records: {len(cdc_records)}")
    print(f" - Injected Anomalies: {len(anomalies)}")
    print(f" - Customer Profiles (SCD 1 & 2): {len(customers)}")


if __name__ == "__main__":
    main()
