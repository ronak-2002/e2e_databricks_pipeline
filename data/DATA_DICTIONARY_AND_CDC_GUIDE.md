# Data Dictionary & Delta Lake CDC / SCD Guide
## E-Commerce Lakehouse Pre-Load & Streaming Portfolio Architecture

This directory contains the historical pre-load dataset designed to bootstrap a **Medallion Architecture (Bronze &rarr; Silver &rarr; Gold)** in **Databricks / Delta Lake**, combining batch baseline data with the live Apache Kafka streaming pipeline.

---

## 1. File Inventory

| File | Format | Record Count | Purpose |
| :--- | :---: | :---: | :--- |
| [`bulk_transactions_preload.json`](file:///c:/Users/hp/e2e%20Databricks/data/bulk_transactions_preload.json) | Standard JSON Array | **824** | Formatted JSON array for IDEs (VS Code) and tools. |
| [`bulk_transactions_preload.jsonl`](file:///c:/Users/hp/e2e%20Databricks/data/bulk_transactions_preload.jsonl) | JSON Lines (NDJSON) | **824** | Line-delimited JSON for distributed big data processing (Spark, Kafka). |
| [`customer_profiles_scd.json`](file:///c:/Users/hp/e2e%20Databricks/data/customer_profiles_scd.json) | Standard JSON Array | **22** | Customer dimension profiles modeling SCD Type 1 & Type 2. |
| [`customer_profiles_scd.jsonl`](file:///c:/Users/hp/e2e%20Databricks/data/customer_profiles_scd.jsonl) | JSON Lines (NDJSON) | **22** | Customer dimension profiles in line-delimited JSON format. |
| [`generate_preload.py`](file:///c:/Users/hp/e2e%20Databricks/data/generate_preload.py) | Python | Script | Generator script for scaling up or re-generating the dataset deterministically. |

---

## 2. Data Dictionary

### A. Transactions Pre-Load (`bulk_transactions_preload.json`)
Each record is a self-contained JSON line representing a transaction event or order state snapshot:

| Field Name | Type | Description | Sample Value |
| :--- | :--- | :--- | :--- |
| `record_id` | `STRING` | Unique ingestion record identifier | `"rec_000001"` |
| `order_id` | `STRING` | Logical business order key | `"ord_hist_10000"` |
| `user_id` | `STRING` | Customer identifier linked to `customer_profiles_scd` | `"usr_alpha_101"` |
| `session_id` | `STRING` | Web/App session identifier | `"sess_9b4e129f"` |
| `transaction_timestamp` | `TIMESTAMP` | When the original transaction occurred (ISO-8601 UTC) | `"2026-11-01T08:15:00+00:00"` |
| `updated_at` | `TIMESTAMP` | Mutation or ingestion timestamp for CDC sequencing | `"2026-11-01T08:30:00+00:00"` |
| `cdc_operation` | `STRING` | Change operation flag (`INSERT`, `UPDATE`, `DELETE`) | `"UPDATE"` |
| `order_status` | `STRING` | Order state (`PENDING`, `CONFIRMED`, `SHIPPED`, `DELIVERED`, `CANCELLED`) | `"CONFIRMED"` |
| `currency` | `STRING` | ISO currency code | `"USD"` |
| `subtotal` | `DECIMAL(10,2)`| Net cart amount before tax and shipping | `398.00` |
| `tax` | `DECIMAL(10,2)`| Calculated sales tax (8%) | `31.84` |
| `shipping_fee` | `DECIMAL(10,2)`| Shipping cost ($0.00 for orders &ge; $100, else $9.99) | `0.00` |
| `total_amount` | `DECIMAL(10,2)`| Final paid amount (`subtotal + tax + shipping - discount`) | `429.84` |
| `discount_amount` | `DECIMAL(10,2)`| Promotional discount deducted | `0.00` |
| `payment_method` | `STRING` | Tender type (`credit_card`, `apple_pay`, `google_pay`, `paypal`) | `"credit_card"` |
| `payment_status` | `STRING` | Gateway state (`CAPTURED`, `PENDING`, `REFUNDED`, `FAILED`) | `"CAPTURED"` |
| `shipping_city` | `STRING` | Destination city | `"San Francisco"` |
| `shipping_state` | `STRING` | Destination state abbreviation | `"CA"` |
| `shipping_country` | `STRING` | Destination country code | `"US"` |
| `device_type` | `STRING` | Client channel (`web`, `ios`, `android`) | `"web"` |
| `items_count` | `INTEGER` | Count of unique product line items | `1` |
| `items` | `ARRAY<STRUCT>`| Line items with `product_id`, `unit_price`, `quantity`, `total_price` | `[{...}]` |
| `data_quality_flags` | `ARRAY<STRING>`| Anomaly and audit tags (`CLEAN`, `ANOMALY_*`, `CDC_STAGE_*`) | `["CLEAN"]` |

---

### B. Customer Profiles Dimension (`customer_profiles_scd.json`)
Dimension table modeling customer identities over time:

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `customer_id` | `STRING` | Natural business key (`usr_alpha_101`, etc.) |
| `name` | `STRING` | Customer full name |
| `email` | `STRING` | Contact email |
| `phone` | `STRING` | Primary phone number |
| `street`, `city`, `state`, `zip`, `country` | `STRING` | Address attributes (subject to SCD 2 relocation tracking) |
| `customer_tier` | `STRING` | Loyalty tier (`BRONZE`, `SILVER`, `GOLD`, `PLATINUM`) |
| `preferred_category` | `STRING` | Primary shopping preference |
| `lifetime_spend` | `DECIMAL(10,2)`| Cumulative spend |
| `is_active` | `BOOLEAN` | Account status flag |
| `version` | `INTEGER` | Dimension record version (1, 2, 3...) |
| `effective_from` | `TIMESTAMP` | Validity start timestamp |
| `effective_to` | `TIMESTAMP` | Validity end timestamp (`9999-12-31T23:59:59Z` for active) |
| `is_current` | `BOOLEAN` | `true` if current active record; `false` if historical |
| `scd_change_reason` | `STRING` | Business trigger for modification |

---

## 3. Real-World Anomalies Injected in Pre-load

The dataset intentionally incorporates **24 real-world data quality anomalies** across distinct categories to demonstrate robust data validation in your portfolio:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       INJECTED ANOMALY INVENTORY                            │
├─────────────────────────┬───────┬───────────────────────────────────────────┤
│ Anomaly Category        │ Count │ Business & Technical Challenge            │
├─────────────────────────┼───────┼───────────────────────────────────────────┤
│ Multi-Step CDC Mutation │  150  │ Out-of-order state merges (PENDING->SHIP) │
│ Late-Arriving Events    │   15  │ Watermarking tests (arrived days late)    │
│ Duplicate Records       │    5  │ Idempotency & deduplication on order_id   │
│ Null / Missing Fields   │    2  │ Data quality quarantine (Null city/device)│
│ Negative Total Amount   │    1  │ Corrupt coupon glitch (Total: -$450.00)   │
│ Extreme Outlier Qty     │    1  │ Bot scraper ordering 10,000 cast irons    │
└─────────────────────────┴───────┴───────────────────────────────────────────┘
```

---

## 4. Databricks & PySpark Implementation Recipes

### Recipe 1: Initializing Delta Lake Bronze Table
```python
# Option A: Read standard JSON array (.json) with multiline option
raw_df = spark.read.option("multiline", "true").json("file:/c:/Users/hp/e2e Databricks/data/bulk_transactions_preload.json")

# Option B: Read line-delimited JSON (.jsonl)
# raw_df = spark.read.json("file:/c:/Users/hp/e2e Databricks/data/bulk_transactions_preload.jsonl")

# Write baseline directly to Bronze Delta Lake table
(raw_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ecom_bronze_transactions"))
```

---

### Recipe 2: CDC Upsert with Delta Lake `MERGE INTO` (Silver Layer)
When real-time updates arrive from Kafka (`ecom.orders`):

```sql
-- Delta Lake SQL MERGE: Handles CDC updates while preserving latest state
MERGE INTO ecom_silver_orders AS target
USING (
    -- Deduplicate incoming batch by latest updated_at per order_id
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) as rn
        FROM streaming_kafka_orders_microbatch
    ) WHERE rn = 1
) AS source
ON target.order_id = source.order_id

-- 1. When matched and incoming update is newer, update state
WHEN MATCHED AND source.updated_at >= target.updated_at THEN
  UPDATE SET
    target.order_status = source.order_status,
    target.payment_status = source.payment_status,
    target.updated_at = source.updated_at,
    target.total_amount = source.total_amount

-- 2. When not matched, insert new order
WHEN NOT MATCHED THEN
  INSERT *;
```

---

### Recipe 3: Implementing SCD Type 2 with Delta Live Tables (DLT)
Using Databricks DLT `apply_changes`:

```python
import dlt
from pyspark.sql.functions import col, expr

@dlt.table
def customer_profiles_bronze():
    return spark.readStream.format("delta").table("customer_profiles_raw")

# Automatically generate SCD Type 2 dimension table
dlt.create_streaming_table("dim_customers_scd2")

dlt.apply_changes(
    target = "dim_customers_scd2",
    source = "customer_profiles_bronze",
    keys = ["customer_id"],
    sequence_by = col("effective_from"),
    ignore_null_updates = False,
    stored_as_scd_type = 2,
    track_history_column_list = ["street", "city", "state", "zip", "customer_tier"]
)
```

---

### Recipe 4: Watermarking for Late-Arriving Events (Streaming Join)
```python
from pyspark.sql.functions import to_timestamp

# Define 2-hour watermark threshold for late-arriving events
streaming_events_with_watermark = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "...")
        .option("subscribe", "ecom.orders")
        .load()
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json("json_payload", event_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp("event_timestamp"))
        .withWatermark("event_time", "2 hours")
)
```

---

### Recipe 5: Data Quality Gate (Quarantine Pattern)
Filter out corrupted pre-load records (negative amounts, extreme quantities):

```python
from pyspark.sql.functions import col

silver_clean_df = raw_df.filter(
    (col("total_amount") > 0) & 
    (col("items_count") > 0) & 
    (col("shipping_city").isNotNull())
)

# Route anomalies into quarantine table for investigation
quarantine_df = raw_df.filter(
    (col("total_amount") <= 0) | 
    (col("items_count") == 0) | 
    (col("shipping_city").isNull())
)
quarantine_df.write.format("delta").mode("append").saveAsTable("ecom_quarantine_transactions")
```
