"""
MeterFlow IQ - 02 Bronze Ingest MongoDB
Connection/count test only.

Purpose:
Confirm that Databricks can connect to MongoDB Atlas and read the seeded
meter_polling_events documents before writing the Bronze Delta table.

This does not write anything to Databricks yet.
"""

# -----------------------------------------------------------------------------
# Ensure required Python libraries are available
# -----------------------------------------------------------------------------

try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi
except ImportError:
    print("Installing pymongo and dnspython...")
    %pip install pymongo dnspython
    import pymongo
    from pymongo import MongoClient
    from pymongo.server_api import ServerApi

# -----------------------------------------------------------------------------
# Notebook parameters
# -----------------------------------------------------------------------------

DEFAULT_DB = "meterflow_iq"
DEFAULT_EVENTS_COLLECTION = "meter_polling_events"
DEFAULT_SEED_BATCH_ID = "MONGO-SEED-001"
EXPECTED_ROWS = 25343

try:
    dbutils.widgets.text("mongodb_uri", "", "MongoDB Atlas URI")
    dbutils.widgets.text("mongodb_db", DEFAULT_DB, "MongoDB database")
    dbutils.widgets.text(
        "mongodb_events_collection",
        DEFAULT_EVENTS_COLLECTION,
        "MongoDB events collection",
    )
    dbutils.widgets.text("seed_batch_id", DEFAULT_SEED_BATCH_ID, "Seed batch ID")

    MONGODB_URI = dbutils.widgets.get("mongodb_uri").strip()
    MONGODB_DB = dbutils.widgets.get("mongodb_db").strip()
    EVENTS_COLLECTION = dbutils.widgets.get("mongodb_events_collection").strip()
    SEED_BATCH_ID = dbutils.widgets.get("seed_batch_id").strip()
except Exception:
    MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
    MONGODB_DB = os.getenv("MONGODB_DB", DEFAULT_DB).strip()
    EVENTS_COLLECTION = os.getenv(
        "MONGODB_EVENTS_COLLECTION",
        DEFAULT_EVENTS_COLLECTION,
    ).strip()
    SEED_BATCH_ID = os.getenv("MONGODB_SEED_BATCH_ID", DEFAULT_SEED_BATCH_ID).strip()

if not MONGODB_URI:
    raise RuntimeError(
        "MongoDB URI is blank. Enter your MongoDB Atlas URI in the "
        "mongodb_uri notebook parameter, then rerun this notebook."
    )

# -----------------------------------------------------------------------------
# Connect and count
# -----------------------------------------------------------------------------

print("Spark version:", spark.version)
print("PyMongo version:", pymongo.version)
print("MongoDB database:", MONGODB_DB)
print("MongoDB events collection:", EVENTS_COLLECTION)
print("Seed batch ID:", SEED_BATCH_ID)
print("Expected seed-batch rows:", EXPECTED_ROWS)

client = MongoClient(
    MONGODB_URI,
    server_api=ServerApi("1"),
    serverSelectionTimeoutMS=15000,
)

client.admin.command("ping")
print("Successfully connected to MongoDB Atlas.")

db = client[MONGODB_DB]
collection = db[EVENTS_COLLECTION]

total_docs = collection.count_documents({})
batch_docs = collection.count_documents({"seed_batch_id": SEED_BATCH_ID})

print("Total docs in collection:", total_docs)
print(f"Docs for seed batch {SEED_BATCH_ID}:", batch_docs)

if batch_docs == EXPECTED_ROWS:
    print("PASS: MongoDB seeded document count matches expected count.")
else:
    print("CHECK: MongoDB seeded document count does not match expected count.")

sample_doc = collection.find_one({"seed_batch_id": SEED_BATCH_ID})

if sample_doc:
    print("Sample document keys:")
    print(sorted(sample_doc.keys()))

    print("Sample event_id:", sample_doc.get("event_id"))
    print("Sample raw_reading_id:", sample_doc.get("raw_reading_id"))
    print("Sample meter_id:", sample_doc.get("meter_id"))
    print("Sample facility_id:", sample_doc.get("facility_id"))
    print("Sample source_system:", sample_doc.get("source_system"))
    print("Sample polling_platform:", sample_doc.get("polling_platform"))
else:
    print("No sample document found for the seed batch.")