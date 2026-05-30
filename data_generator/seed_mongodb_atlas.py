from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi


DEFAULT_DB = "meterflow_iq"
DEFAULT_EVENTS_COLLECTION = "meter_polling_events"
DEFAULT_SEED_RUNS_COLLECTION = "seed_runs"
DEFAULT_SEED_BATCH_ID = "MONGO-SEED-001"
DEFAULT_NORMAL_SAMPLE = 25000
CHUNK_SIZE = 1000

ACCEPTED_QUALITY_CODES = {"GOOD", "ESTIMATED", "QUESTIONABLE", "STALE", "MISSING"}
NORMAL_STATUSES = {"VALID", "REVIEW"}

RAW_STATUS_TO_SCENARIO = {
    "NO_SIGNAL": "SCN-001",
    "DUPLICATE_RETRY": "SCN-002",
    "INVALID_METER": "SCN-003",
    "LATE_ARRIVAL": "SCN-004",
    "STALE": "SCN-005",
    "FUTURE_DATE": "SCN-006",
    "FACILITY_MISMATCH": "SCN-010",
    "INVALID_QUALITY_CODE": "SCN-011",
    "NULL_VOLUME": "SCN-013",
    "NEGATIVE_VOLUME": "SCN-014",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_env() -> None:
    # Loads .env if present. Environment variables still win.
    load_dotenv(repo_root() / ".env")


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def as_none_if_nan(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(tzinfo=timezone.utc)
    return value


def to_iso(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def to_float_or_none(value: Any) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def load_source_frames(sample_data_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_path = sample_data_dir / "raw_polling_readings.csv"
    meter_path = sample_data_dir / "meter_master.csv"
    scenario_path = sample_data_dir / "known_issue_scenarios.csv"

    for path in (raw_path, meter_path, scenario_path):
        if not path.exists():
            raise FileNotFoundError(f"Missing required source file: {path}")

    raw = pd.read_csv(raw_path)
    meters = pd.read_csv(meter_path)
    scenarios = pd.read_csv(scenario_path)

    for col in ["reading_timestamp", "poll_timestamp", "load_timestamp"]:
        raw[col] = pd.to_datetime(raw[col], errors="coerce")

    raw["volume_num"] = pd.to_numeric(raw["volume"], errors="coerce")
    raw["pressure_num"] = pd.to_numeric(raw["pressure"], errors="coerce")
    raw["temperature_num"] = pd.to_numeric(raw["temperature"], errors="coerce")

    return raw, meters, scenarios


def select_seed_rows(raw: pd.DataFrame, normal_sample: int, anomaly_only: bool) -> pd.DataFrame:
    anomaly_mask = (
        raw["volume_num"].isna()
        | raw["volume_num"].eq(0)
        | raw["volume_num"].lt(0)
        | ~raw["quality_code"].isin(ACCEPTED_QUALITY_CODES)
        | ~raw["raw_status"].isin(NORMAL_STATUSES)
    )

    anomalies = raw.loc[anomaly_mask].copy()

    if anomaly_only or normal_sample <= 0:
        selected = anomalies
    else:
        normal_pool = raw.loc[~anomaly_mask].copy()
        sample_n = min(normal_sample, len(normal_pool))
        normal_sample_df = normal_pool.sample(n=sample_n, random_state=42) if sample_n else normal_pool.head(0)
        selected = pd.concat([anomalies, normal_sample_df], ignore_index=True)

    selected = selected.drop_duplicates(subset=["raw_reading_id"]).sort_values("raw_reading_id")
    return selected


def event_context(row: pd.Series) -> dict[str, Any]:
    raw_status = str(row.get("raw_status", ""))
    quality_code = str(row.get("quality_code", ""))
    volume = to_float_or_none(row.get("volume_num"))

    device_status = "ONLINE"
    communication_status = "NORMAL"
    battery_status = "OK"
    polling_error_code = None
    signal_quality = "GOOD"
    retry_count = 0

    if raw_status == "NO_SIGNAL":
        device_status = "NO_SIGNAL"
        communication_status = "FAILED"
        polling_error_code = "COMMUNICATION_FAILURE"
        signal_quality = "NONE"
        battery_status = "LOW"
    elif raw_status == "STALE" or quality_code == "STALE":
        device_status = "ONLINE"
        communication_status = "STALE"
        polling_error_code = "STALE_TAG"
        signal_quality = "STALE"
    elif raw_status == "LATE_ARRIVAL":
        device_status = "ONLINE"
        communication_status = "DELAYED"
        polling_error_code = "LATE_ARRIVAL"
        signal_quality = "GOOD"
    elif raw_status == "DUPLICATE_RETRY":
        communication_status = "RETRY_DUPLICATE"
        polling_error_code = "DUPLICATE_RETRY"
        retry_count = 1
    elif raw_status == "INVALID_METER":
        device_status = "UNKNOWN_DEVICE"
        communication_status = "UNKNOWN"
        polling_error_code = "METER_NOT_IN_MASTER"
        signal_quality = "UNKNOWN"
    elif raw_status == "INVALID_QUALITY_CODE" or quality_code not in ACCEPTED_QUALITY_CODES:
        polling_error_code = "INVALID_QUALITY_CODE"
        signal_quality = "UNKNOWN"
    elif raw_status == "NULL_VOLUME" or volume is None:
        polling_error_code = "NULL_VOLUME"
    elif raw_status == "NEGATIVE_VOLUME":
        device_status = "SENSOR_ANOMALY"
        polling_error_code = "NEGATIVE_VOLUME"
        signal_quality = "BAD"
    elif raw_status == "FACILITY_MISMATCH":
        polling_error_code = "FACILITY_MISMATCH"
    elif raw_status == "FUTURE_DATE":
        polling_error_code = "FUTURE_DATE"

    return {
        "device_status": device_status,
        "communication_status": communication_status,
        "battery_status": battery_status,
        "polling_error_code": polling_error_code,
        "signal_quality": signal_quality,
        "retry_count": retry_count,
    }


def scenario_id_for_row(row: pd.Series) -> str | None:
    raw_status = str(row.get("raw_status", ""))
    return RAW_STATUS_TO_SCENARIO.get(raw_status)


def build_event_document(row: pd.Series, seed_batch_id: str) -> dict[str, Any]:
    raw_reading_id = str(row["raw_reading_id"])
    event_id = f"EVT-{raw_reading_id}"
    context = event_context(row)

    event_timestamp = pd.Timestamp(row["reading_timestamp"]).to_pydatetime().replace(tzinfo=timezone.utc)
    poll_timestamp = pd.Timestamp(row["poll_timestamp"]).to_pydatetime().replace(tzinfo=timezone.utc)
    load_timestamp = pd.Timestamp(row["load_timestamp"]).to_pydatetime().replace(tzinfo=timezone.utc)

    payload = {
        "volume": to_float_or_none(row.get("volume_num")),
        "pressure": to_float_or_none(row.get("pressure_num")),
        "temperature": to_float_or_none(row.get("temperature_num")),
        "quality_code": as_none_if_nan(row.get("quality_code")),
        "raw_status": as_none_if_nan(row.get("raw_status")),
        "device_status": context["device_status"],
        "communication_status": context["communication_status"],
        "battery_status": context["battery_status"],
        "polling_error_code": context["polling_error_code"],
        "signal_quality": context["signal_quality"],
        "retry_count": context["retry_count"],
        "schema_version": "meterflow_event_v1",
    }

    return {
        "event_id": event_id,
        "raw_reading_id": raw_reading_id,
        "source_system": as_none_if_nan(row.get("source_system")),
        "polling_platform": as_none_if_nan(row.get("polling_platform")),
        "meter_id": as_none_if_nan(row.get("meter_id")),
        "facility_id": as_none_if_nan(row.get("facility_id")),
        "event_timestamp": event_timestamp,
        "production_date": as_none_if_nan(row.get("production_date")),
        "gas_day": as_none_if_nan(row.get("gas_day")),
        "poll_timestamp": poll_timestamp,
        "load_timestamp": load_timestamp,
        "payload_type": "meter_reading",
        "payload": payload,
        "raw_message_source": "seed_mongodb_atlas.py",
        "source_file": "raw_polling_readings.csv",
        "source_batch_id": as_none_if_nan(row.get("batch_id")),
        "seed_batch_id": seed_batch_id,
        "scenario_id": scenario_id_for_row(row),
        "ingested_at": datetime.now(timezone.utc),
    }


def ensure_indexes(db, events_collection_name: str, seed_runs_collection_name: str) -> None:
    events = db[events_collection_name]
    seed_runs = db[seed_runs_collection_name]

    events.create_index("event_id", unique=True)
    events.create_index("raw_reading_id")
    events.create_index("seed_batch_id")
    events.create_index([("meter_id", 1), ("event_timestamp", 1)])
    events.create_index([("facility_id", 1), ("event_timestamp", 1)])
    events.create_index("source_batch_id")
    events.create_index("scenario_id")

    seed_runs.create_index("seed_run_id", unique=True)
    seed_runs.create_index("seed_batch_id")


def chunked(items: list[Any], chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i : i + chunk_size]


def upsert_events(collection, docs: list[dict[str, Any]]) -> dict[str, int]:
    inserted = 0
    matched = 0
    modified = 0

    for doc_chunk in chunked(docs, CHUNK_SIZE):
        ops = [
            UpdateOne(
                {"event_id": doc["event_id"]},
                {
                    "$setOnInsert": doc,
                    "$set": {
                        "last_seed_attempt_at": datetime.now(timezone.utc),
                        "last_seed_batch_id": doc["seed_batch_id"],
                    },
                },
                upsert=True,
            )
            for doc in doc_chunk
        ]
        result = collection.bulk_write(ops, ordered=False)
        inserted += result.upserted_count
        matched += result.matched_count
        modified += result.modified_count

    return {"inserted": inserted, "matched": matched, "modified": modified}


def print_dry_run_summary(selected: pd.DataFrame, docs: list[dict[str, Any]]) -> None:
    print("Dry run only. No MongoDB writes performed.")
    print(f"Selected rows: {len(selected):,}")
    print("Raw status breakdown:")
    print(selected["raw_status"].value_counts(dropna=False).head(20).to_string())
    print("\nSample MongoDB document:")
    sample_doc = docs[0].copy() if docs else {}
    if sample_doc:
        # Convert datetimes for display only.
        for key in ["event_timestamp", "poll_timestamp", "load_timestamp", "ingested_at"]:
            if key in sample_doc and hasattr(sample_doc[key], "isoformat"):
                sample_doc[key] = sample_doc[key].isoformat()
    print(json.dumps(sample_doc, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed MongoDB Atlas with MeterFlow IQ raw event documents.")
    parser.add_argument("--sample-data-dir", default=str(repo_root() / "sample_data"), help="Folder containing generated CSVs.")
    parser.add_argument("--seed-batch-id", default=DEFAULT_SEED_BATCH_ID, help="Seeder batch identifier for tracking/reset.")
    parser.add_argument("--normal-sample", type=int, default=DEFAULT_NORMAL_SAMPLE, help="Number of normal records to seed in addition to all anomalies.")
    parser.add_argument("--anomaly-only", action="store_true", help="Seed only anomalous records.")
    parser.add_argument("--reset", action="store_true", help="Delete existing docs for this seed_batch_id before seeding.")
    parser.add_argument("--dry-run", action="store_true", help="Show selected rows and sample document without writing to MongoDB.")
    parser.add_argument("--stats", action="store_true", help="Print MongoDB collection counts and exit.")
    args = parser.parse_args()

    load_env()

    sample_data_dir = Path(args.sample_data_dir)
    raw, _meters, _scenarios = load_source_frames(sample_data_dir)
    selected = select_seed_rows(raw, normal_sample=args.normal_sample, anomaly_only=args.anomaly_only)
    docs = [build_event_document(row, args.seed_batch_id) for _, row in selected.iterrows()]

    if args.dry_run:
        print_dry_run_summary(selected, docs)
        return

    mongo_uri = require_env("MONGODB_URI")
    db_name = os.environ.get("MONGODB_DB", DEFAULT_DB)
    events_collection_name = os.environ.get("MONGODB_EVENTS_COLLECTION", DEFAULT_EVENTS_COLLECTION)
    seed_runs_collection_name = os.environ.get("MONGODB_SEED_RUNS_COLLECTION", DEFAULT_SEED_RUNS_COLLECTION)

    client = MongoClient(mongo_uri, server_api=ServerApi("1"), serverSelectionTimeoutMS=10000)
    client.admin.command("ping")

    db = client[db_name]
    ensure_indexes(db, events_collection_name, seed_runs_collection_name)

    events = db[events_collection_name]
    seed_runs = db[seed_runs_collection_name]

    if args.stats:
        print(f"Database: {db_name}")
        print(f"Events collection: {events_collection_name}")
        print(f"Total event docs: {events.count_documents({}):,}")
        print(f"Docs for seed batch {args.seed_batch_id}: {events.count_documents({'seed_batch_id': args.seed_batch_id}):,}")
        print(f"Seed run docs: {seed_runs.count_documents({}):,}")
        return

    started_at = datetime.now(timezone.utc)

    if args.reset:
        deleted = events.delete_many({"seed_batch_id": args.seed_batch_id}).deleted_count
        print(f"Reset requested. Deleted {deleted:,} existing docs for seed_batch_id={args.seed_batch_id}.")

    result_counts = upsert_events(events, docs)
    completed_at = datetime.now(timezone.utc)

    seed_run_id = f"SEED-RUN-{args.seed_batch_id}"
    seed_run_doc = {
        "seed_run_id": seed_run_id,
        "seed_batch_id": args.seed_batch_id,
        "source_file": "raw_polling_readings.csv",
        "started_at": started_at,
        "completed_at": completed_at,
        "selected_rows": len(selected),
        "normal_sample_requested": args.normal_sample,
        "anomaly_only": args.anomaly_only,
        "inserted_count": result_counts["inserted"],
        "matched_count": result_counts["matched"],
        "modified_count": result_counts["modified"],
        "status": "SUCCESS",
    }

    seed_runs.update_one(
        {"seed_run_id": seed_run_id},
        {"$set": seed_run_doc},
        upsert=True,
    )

    print("MongoDB Atlas seeding complete.")
    print(f"Database: {db_name}")
    print(f"Events collection: {events_collection_name}")
    print(f"Seed batch: {args.seed_batch_id}")
    print(f"Selected rows: {len(selected):,}")
    print(f"Inserted new docs: {result_counts['inserted']:,}")
    print(f"Matched existing docs: {result_counts['matched']:,}")
    print(f"Modified existing docs: {result_counts['modified']:,}")
    print(f"Total docs in collection: {events.count_documents({}):,}")


if __name__ == "__main__":
    main()
