"""
MeterFlow IQ - 02 Bronze Ingest MongoDB

Purpose:
Read raw semi-structured polling/device event documents from MongoDB Atlas,
normalize them into a Bronze-friendly tabular structure, preserve the raw JSON
document, add Bronze audit metadata, write a managed Delta table, and validate
row counts.

Bronze rule:
Do not clean or fix records here. Bronze preserves source context and raw
payloads so Silver can flatten, standardize, validate, and flag issues later.
"""

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, date
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# -----------------------------------------------------------------------------
# Ensure required Python libraries are available
# -----------------------------------------------------------------------------

try:
    from bson import ObjectId
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
except ImportError:
    print("Installing pymongo and dnspython for MongoDB Atlas access...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "pymongo", "dnspython"]
    )
    from bson import ObjectId
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

BRONZE_SCHEMA = "meterflow_iq_bronze"
TARGET_TABLE = "bronze_meter_polling_events"
BRONZE_RUN_ID = f"bronze_mongo_{uuid.uuid4().hex[:12]}"

DEFAULT_DB = "meterflow_iq"
DEFAULT_EVENTS_COLLECTION = "meter_polling_events"
DEFAULT_SEED_BATCH_ID = "MONGO-SEED-001"
DEFAULT_EXPECTED_ROWS = "25343"

# -----------------------------------------------------------------------------
# Notebook parameters
# -----------------------------------------------------------------------------

def get_or_create_text_widget(name: str, default_value: str, label: str) -> str:
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        dbutils.widgets.text(name, default_value, label)
        return dbutils.widgets.get(name).strip()

try:
    MONGODB_URI = get_or_create_text_widget("mongodb_uri", "", "MongoDB Atlas URI")
    MONGODB_DB = get_or_create_text_widget("mongodb_db", DEFAULT_DB, "MongoDB database")
    EVENTS_COLLECTION = get_or_create_text_widget("mongodb_events_collection", DEFAULT_EVENTS_COLLECTION, "MongoDB events collection")
    SEED_BATCH_ID = get_or_create_text_widget("seed_batch_id", DEFAULT_SEED_BATCH_ID, "Seed batch ID")
    EXPECTED_ROWS_TEXT = get_or_create_text_widget("expected_rows", DEFAULT_EXPECTED_ROWS, "Expected rows")
    EXPECTED_ROWS = int(EXPECTED_ROWS_TEXT or DEFAULT_EXPECTED_ROWS)
except Exception:
    MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
    MONGODB_DB = os.getenv("MONGODB_DB", DEFAULT_DB).strip()
    EVENTS_COLLECTION = os.getenv("MONGODB_EVENTS_COLLECTION", DEFAULT_EVENTS_COLLECTION).strip()
    SEED_BATCH_ID = os.getenv("MONGODB_SEED_BATCH_ID", DEFAULT_SEED_BATCH_ID).strip()
    EXPECTED_ROWS = int(os.getenv("MONGODB_EXPECTED_ROWS", DEFAULT_EXPECTED_ROWS))

if not MONGODB_URI:
    raise RuntimeError(
        "MONGODB_URI is not set. In Databricks, enter your MongoDB Atlas URI "
        "in the mongodb_uri notebook parameter, then rerun this notebook."
    )

print("Spark version:", spark.version)
print("Bronze run ID:", BRONZE_RUN_ID)
print("MongoDB database:", MONGODB_DB)
print("MongoDB events collection:", EVENTS_COLLECTION)
print("Seed batch ID:", SEED_BATCH_ID)
print("Expected rows:", EXPECTED_ROWS)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def to_json_safe(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: to_json_safe(val) for key, val in value.items()}
    if isinstance(value, list):
        return [to_json_safe(item) for item in value]
    return value

def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def safe_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)

def normalize_mongo_document(doc: dict[str, Any]) -> dict[str, Any]:
    safe_doc = to_json_safe(doc)
    payload = safe_doc.get("payload") or {}
    return {
        "mongo_object_id": safe_str(safe_doc.get("_id")),
        "event_id": safe_str(safe_doc.get("event_id")),
        "raw_reading_id": safe_str(safe_doc.get("raw_reading_id")),
        "source_system": safe_str(safe_doc.get("source_system")),
        "polling_platform": safe_str(safe_doc.get("polling_platform")),
        "meter_id": safe_str(safe_doc.get("meter_id")),
        "facility_id": safe_str(safe_doc.get("facility_id")),
        "event_timestamp": safe_str(safe_doc.get("event_timestamp")),
        "production_date": safe_str(safe_doc.get("production_date")),
        "gas_day": safe_str(safe_doc.get("gas_day")),
        "poll_timestamp": safe_str(safe_doc.get("poll_timestamp")),
        "load_timestamp": safe_str(safe_doc.get("load_timestamp")),
        "payload_type": safe_str(safe_doc.get("payload_type")),
        "payload_volume": safe_float(payload.get("volume")),
        "payload_pressure": safe_float(payload.get("pressure")),
        "payload_temperature": safe_float(payload.get("temperature")),
        "payload_quality_code": safe_str(payload.get("quality_code")),
        "payload_raw_status": safe_str(payload.get("raw_status")),
        "payload_device_status": safe_str(payload.get("device_status")),
        "payload_communication_status": safe_str(payload.get("communication_status")),
        "payload_battery_status": safe_str(payload.get("battery_status")),
        "payload_polling_error_code": safe_str(payload.get("polling_error_code")),
        "payload_signal_quality": safe_str(payload.get("signal_quality")),
        "payload_retry_count": safe_int(payload.get("retry_count")),
        "payload_schema_version": safe_str(payload.get("schema_version")),
        "raw_message_source": safe_str(safe_doc.get("raw_message_source")),
        "source_file": safe_str(safe_doc.get("source_file")),
        "source_batch_id": safe_str(safe_doc.get("source_batch_id")),
        "seed_batch_id": safe_str(safe_doc.get("seed_batch_id")),
        "scenario_id": safe_str(safe_doc.get("scenario_id")),
        "ingested_at": safe_str(safe_doc.get("ingested_at")),
        "last_seed_attempt_at": safe_str(safe_doc.get("last_seed_attempt_at")),
        "last_seed_batch_id": safe_str(safe_doc.get("last_seed_batch_id")),
        "payload_json": json.dumps(payload, sort_keys=True),
        "raw_document_json": json.dumps(safe_doc, sort_keys=True),
    }

# -----------------------------------------------------------------------------
# Connect to MongoDB Atlas and read documents
# -----------------------------------------------------------------------------

client = MongoClient(
    MONGODB_URI,
    server_api=ServerApi("1"),
    serverSelectionTimeoutMS=15000,
)

client.admin.command("ping")
print("Successfully connected to MongoDB Atlas.")

db = client[MONGODB_DB]
collection = db[EVENTS_COLLECTION]

total_collection_count = collection.count_documents({})
query = {"seed_batch_id": SEED_BATCH_ID}
source_count = collection.count_documents(query)

print("Total docs in collection:", total_collection_count)
print(f"Docs for seed batch {SEED_BATCH_ID}:", source_count)

if source_count == 0:
    raise RuntimeError(
        f"No MongoDB event documents found for seed_batch_id={SEED_BATCH_ID}."
    )

docs = list(collection.find(query))
rows = [normalize_mongo_document(doc) for doc in docs]

print("Rows normalized for Spark:", len(rows))

# -----------------------------------------------------------------------------
# Create Spark DataFrame
# -----------------------------------------------------------------------------

mongo_event_schema = StructType(
    [
        StructField("mongo_object_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("raw_reading_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("polling_platform", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("facility_id", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("production_date", StringType(), True),
        StructField("gas_day", StringType(), True),
        StructField("poll_timestamp", StringType(), True),
        StructField("load_timestamp", StringType(), True),
        StructField("payload_type", StringType(), True),
        StructField("payload_volume", DoubleType(), True),
        StructField("payload_pressure", DoubleType(), True),
        StructField("payload_temperature", DoubleType(), True),
        StructField("payload_quality_code", StringType(), True),
        StructField("payload_raw_status", StringType(), True),
        StructField("payload_device_status", StringType(), True),
        StructField("payload_communication_status", StringType(), True),
        StructField("payload_battery_status", StringType(), True),
        StructField("payload_polling_error_code", StringType(), True),
        StructField("payload_signal_quality", StringType(), True),
        StructField("payload_retry_count", IntegerType(), True),
        StructField("payload_schema_version", StringType(), True),
        StructField("raw_message_source", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_batch_id", StringType(), True),
        StructField("seed_batch_id", StringType(), True),
        StructField("scenario_id", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("last_seed_attempt_at", StringType(), True),
        StructField("last_seed_batch_id", StringType(), True),
        StructField("payload_json", StringType(), True),
        StructField("raw_document_json", StringType(), True),
    ]
)

raw_mongo_df = spark.createDataFrame(rows, schema=mongo_event_schema)

source_df_count = raw_mongo_df.count()
print("Spark DataFrame source rows:", source_df_count)

display(raw_mongo_df.limit(5))

# -----------------------------------------------------------------------------
# Add Bronze metadata
# -----------------------------------------------------------------------------

bronze_df = (
    raw_mongo_df
    .withColumn("_record_hash", F.sha2(F.col("raw_document_json"), 256))
    .withColumn("_bronze_run_id", F.lit(BRONZE_RUN_ID))
    .withColumn("_source_type", F.lit("mongodb_atlas"))
    .withColumn("_source_database", F.lit(MONGODB_DB))
    .withColumn("_source_collection", F.lit(EVENTS_COLLECTION))
    .withColumn("_source_filter", F.lit(f"seed_batch_id={SEED_BATCH_ID}"))
    .withColumn("_ingested_at", F.current_timestamp())
)

# -----------------------------------------------------------------------------
# Write Bronze Delta table
# -----------------------------------------------------------------------------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"USE {BRONZE_SCHEMA}")

full_table_name = f"{BRONZE_SCHEMA}.{TARGET_TABLE}"

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(full_table_name)
)

written_rows = spark.table(full_table_name).count()

status = (
    "PASS"
    if source_count == EXPECTED_ROWS and written_rows == EXPECTED_ROWS
    else "CHECK"
)

summary_rows = [
    {
        "source_database": MONGODB_DB,
        "source_collection": EVENTS_COLLECTION,
        "seed_batch_id": SEED_BATCH_ID,
        "target_table": full_table_name,
        "expected_rows": EXPECTED_ROWS,
        "source_rows": source_count,
        "spark_dataframe_rows": source_df_count,
        "written_rows": written_rows,
        "status": status,
    }
]

summary_df = spark.createDataFrame(summary_rows)

print("=" * 90)
print("Bronze MongoDB ingest complete.")
print("Target table:", full_table_name)
print("Expected rows:", EXPECTED_ROWS)
print("Source rows:", source_count)
print("Spark DataFrame rows:", source_df_count)
print("Written rows:", written_rows)
print("Status:", status)

display(summary_df)

if status != "PASS":
    print("WARNING: Bronze MongoDB ingest row-count check needs review.")
else:
    print("Bronze MongoDB row-count check passed.")

# -----------------------------------------------------------------------------
# Show created Bronze table
# -----------------------------------------------------------------------------

print("Created / updated Bronze MongoDB table:")
display(spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA} LIKE '{TARGET_TABLE}'"))

print("Sample Bronze MongoDB records:")
display(spark.table(full_table_name).limit(10))