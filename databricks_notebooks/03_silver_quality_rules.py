# MeterFlow IQ - 03 Silver Quality Rules
# Purpose: Create standardized Silver tables from Bronze CSV and MongoDB sources.

import uuid
from functools import reduce

from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

BRONZE_SCHEMA = "meterflow_iq_bronze"
SILVER_SCHEMA = "meterflow_iq_silver"
SILVER_RUN_ID = f"silver_quality_{uuid.uuid4().hex[:12]}"

ACCEPTED_QUALITY_CODES = ["GOOD", "ESTIMATED", "QUESTIONABLE", "STALE", "MISSING"]

print("Spark version:", spark.version)
print("Silver run ID:", SILVER_RUN_ID)
print("Bronze schema:", BRONZE_SCHEMA)
print("Silver schema:", SILVER_SCHEMA)

# -----------------------------------------------------------------------------
# Create Silver schema/database
# -----------------------------------------------------------------------------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"USE {SILVER_SCHEMA}")

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def yes_no_to_boolean(column_name: str):
    return (
        F.when(F.upper(F.trim(F.col(column_name).cast("string"))) == "Y", F.lit(True))
        .when(F.upper(F.trim(F.col(column_name).cast("string"))) == "N", F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )

def clean_string(column_name: str):
    return (
        F.when(F.trim(F.col(column_name).cast("string")) == "", F.lit(None))
        .otherwise(F.trim(F.col(column_name).cast("string")))
    )

def add_silver_metadata(df, source_table: str):
    return (
        df
        .withColumn("_silver_run_id", F.lit(SILVER_RUN_ID))
        .withColumn("_silver_source_table", F.lit(source_table))
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_silver_record_hash", F.sha2(F.to_json(F.struct("*")), 256))
    )

def write_silver_table(df, table_name: str) -> int:
    full_table_name = f"{SILVER_SCHEMA}.{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(full_table_name)
    )
    row_count = spark.table(full_table_name).count()
    print(f"Wrote {full_table_name}: {row_count:,} rows")
    return row_count

def table(full_table_name: str):
    return spark.table(full_table_name)

def bool_or_false(column_expr):
    return F.coalesce(column_expr, F.lit(False))

# -----------------------------------------------------------------------------
# Read Bronze tables
# -----------------------------------------------------------------------------

print("=" * 90)
print("Reading Bronze tables...")

bronze_facility = table(f"{BRONZE_SCHEMA}.bronze_facility_master")
bronze_meter = table(f"{BRONZE_SCHEMA}.bronze_meter_master")
bronze_raw = table(f"{BRONZE_SCHEMA}.bronze_raw_polling_readings")
bronze_mongo = table(f"{BRONZE_SCHEMA}.bronze_meter_polling_events")
bronze_flowcal = table(f"{BRONZE_SCHEMA}.bronze_flowcal_measurement_extract")
bronze_nominations = table(f"{BRONZE_SCHEMA}.bronze_nominations_daily")
bronze_tickets = table(f"{BRONZE_SCHEMA}.bronze_support_tickets")
bronze_pipeline = table(f"{BRONZE_SCHEMA}.bronze_pipeline_run_log")
bronze_dq_rules = table(f"{BRONZE_SCHEMA}.bronze_dq_rules_reference")
bronze_scenarios = table(f"{BRONZE_SCHEMA}.bronze_known_issue_scenarios")

print("Bronze tables loaded.")

# -----------------------------------------------------------------------------
# Standardize reference/master tables
# -----------------------------------------------------------------------------

silver_facility = (
    bronze_facility
    .select(
        clean_string("facility_id").alias("facility_id"),
        clean_string("facility_name").alias("facility_name"),
        clean_string("region").alias("region"),
        clean_string("basin").alias("basin"),
        clean_string("state").alias("state"),
        clean_string("asset_type").alias("asset_type"),
        clean_string("operator").alias("operator"),
        F.upper(clean_string("active_flag")).alias("active_flag"),
        yes_no_to_boolean("active_flag").alias("is_active"),
        F.to_date(clean_string("effective_start_date")).alias("effective_start_date"),
        F.to_date(clean_string("effective_end_date")).alias("effective_end_date"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        clean_string("source_system").alias("source_system"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_facility = add_silver_metadata(
    silver_facility,
    f"{BRONZE_SCHEMA}.bronze_facility_master",
)

silver_meter = (
    bronze_meter
    .select(
        clean_string("meter_id").alias("meter_id"),
        clean_string("facility_id").alias("facility_id"),
        clean_string("meter_name").alias("meter_name"),
        clean_string("meter_type").alias("meter_type"),
        clean_string("product").alias("product"),
        clean_string("measurement_type").alias("measurement_type"),
        F.upper(clean_string("custody_transfer_flag")).alias("custody_transfer_flag"),
        yes_no_to_boolean("custody_transfer_flag").alias("is_custody_transfer"),
        clean_string("source_system").alias("source_system"),
        clean_string("polling_platform").alias("polling_platform"),
        F.to_date(clean_string("install_date")).alias("install_date"),
        F.upper(clean_string("active_flag")).alias("active_flag"),
        yes_no_to_boolean("active_flag").alias("is_active"),
        F.col("expected_min_volume").cast("double").alias("expected_min_volume"),
        F.col("expected_max_volume").cast("double").alias("expected_max_volume"),
        F.col("sample_interval_minutes").cast("int").alias("sample_interval_minutes"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_meter = add_silver_metadata(
    silver_meter,
    f"{BRONZE_SCHEMA}.bronze_meter_master",
)

# -----------------------------------------------------------------------------
# Standardize raw polling readings
# -----------------------------------------------------------------------------

silver_raw_readings = (
    bronze_raw
    .select(
        clean_string("raw_reading_id").alias("raw_reading_id"),
        clean_string("meter_id").alias("meter_id"),
        clean_string("facility_id").alias("source_facility_id"),
        F.to_timestamp(clean_string("reading_timestamp")).alias("reading_timestamp"),
        F.to_date(clean_string("production_date")).alias("production_date"),
        F.to_date(clean_string("gas_day")).alias("gas_day"),
        F.to_timestamp(clean_string("poll_timestamp")).alias("poll_timestamp"),
        clean_string("source_system").alias("source_system"),
        clean_string("polling_platform").alias("polling_platform"),
        F.col("volume").cast("double").alias("volume"),
        F.col("pressure").cast("double").alias("pressure"),
        F.col("temperature").cast("double").alias("temperature"),
        F.upper(clean_string("quality_code")).alias("quality_code"),
        F.upper(clean_string("raw_status")).alias("raw_status"),
        F.to_timestamp(clean_string("load_timestamp")).alias("load_timestamp"),
        clean_string("batch_id").alias("batch_id"),
        F.col("_bronze_run_id"),
        F.col("_source_file"),
        F.col("_source_path"),
        F.col("_source_type"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_raw_readings = add_silver_metadata(
    silver_raw_readings,
    f"{BRONZE_SCHEMA}.bronze_raw_polling_readings",
)

# -----------------------------------------------------------------------------
# Standardize MongoDB raw event table
# -----------------------------------------------------------------------------

silver_mongo_events = (
    bronze_mongo
    .select(
        clean_string("mongo_object_id").alias("mongo_object_id"),
        clean_string("event_id").alias("event_id"),
        clean_string("raw_reading_id").alias("raw_reading_id"),
        clean_string("source_system").alias("source_system"),
        clean_string("polling_platform").alias("polling_platform"),
        clean_string("meter_id").alias("meter_id"),
        clean_string("facility_id").alias("facility_id"),
        F.to_timestamp(clean_string("event_timestamp")).alias("event_timestamp"),
        F.to_date(clean_string("production_date")).alias("production_date"),
        F.to_date(clean_string("gas_day")).alias("gas_day"),
        F.to_timestamp(clean_string("poll_timestamp")).alias("poll_timestamp"),
        F.to_timestamp(clean_string("load_timestamp")).alias("load_timestamp"),
        clean_string("payload_type").alias("payload_type"),
        F.col("payload_volume").cast("double").alias("payload_volume"),
        F.col("payload_pressure").cast("double").alias("payload_pressure"),
        F.col("payload_temperature").cast("double").alias("payload_temperature"),
        F.upper(clean_string("payload_quality_code")).alias("payload_quality_code"),
        F.upper(clean_string("payload_raw_status")).alias("payload_raw_status"),
        F.upper(clean_string("payload_device_status")).alias("payload_device_status"),
        F.upper(clean_string("payload_communication_status")).alias("payload_communication_status"),
        F.upper(clean_string("payload_battery_status")).alias("payload_battery_status"),
        clean_string("payload_polling_error_code").alias("payload_polling_error_code"),
        F.upper(clean_string("payload_signal_quality")).alias("payload_signal_quality"),
        F.col("payload_retry_count").cast("int").alias("payload_retry_count"),
        clean_string("payload_schema_version").alias("payload_schema_version"),
        clean_string("raw_message_source").alias("raw_message_source"),
        clean_string("source_file").alias("source_file"),
        clean_string("source_batch_id").alias("source_batch_id"),
        clean_string("seed_batch_id").alias("seed_batch_id"),
        clean_string("scenario_id").alias("scenario_id"),
        F.to_timestamp(clean_string("ingested_at")).alias("mongo_ingested_at"),
        F.to_timestamp(clean_string("last_seed_attempt_at")).alias("last_seed_attempt_at"),
        clean_string("last_seed_batch_id").alias("last_seed_batch_id"),
        clean_string("payload_json").alias("payload_json"),
        clean_string("raw_document_json").alias("raw_document_json"),
        F.col("_bronze_run_id"),
        F.col("_source_type"),
        F.col("_source_database"),
        F.col("_source_collection"),
        F.col("_source_filter"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_mongo_events = add_silver_metadata(
    silver_mongo_events,
    f"{BRONZE_SCHEMA}.bronze_meter_polling_events",
)

# -----------------------------------------------------------------------------
# Standardize other supporting source tables
# -----------------------------------------------------------------------------

silver_flowcal = (
    bronze_flowcal
    .select(
        clean_string("flowcal_record_id").alias("flowcal_record_id"),
        clean_string("meter_id").alias("meter_id"),
        clean_string("facility_id").alias("facility_id"),
        F.to_date(clean_string("production_date")).alias("production_date"),
        F.to_date(clean_string("gas_day")).alias("gas_day"),
        F.col("measured_volume").cast("double").alias("measured_volume"),
        F.col("corrected_volume").cast("double").alias("corrected_volume"),
        F.upper(clean_string("estimated_flag")).alias("estimated_flag"),
        yes_no_to_boolean("estimated_flag").alias("is_estimated"),
        F.upper(clean_string("validation_status")).alias("validation_status"),
        F.upper(clean_string("exception_code")).alias("exception_code"),
        F.upper(clean_string("approved_flag")).alias("approved_flag"),
        yes_no_to_boolean("approved_flag").alias("is_approved"),
        F.upper(clean_string("close_status")).alias("close_status"),
        clean_string("flowcal_batch_id").alias("flowcal_batch_id"),
        F.to_timestamp(clean_string("extracted_timestamp")).alias("extracted_timestamp"),
        F.to_timestamp(clean_string("last_updated_timestamp")).alias("last_updated_timestamp"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_flowcal = add_silver_metadata(
    silver_flowcal,
    f"{BRONZE_SCHEMA}.bronze_flowcal_measurement_extract",
)

silver_nominations = (
    bronze_nominations
    .select(
        clean_string("nomination_id").alias("nomination_id"),
        clean_string("facility_id").alias("facility_id"),
        F.to_date(clean_string("production_date")).alias("production_date"),
        clean_string("product").alias("product"),
        F.col("nominated_volume").cast("double").alias("nominated_volume"),
        clean_string("customer_group").alias("customer_group"),
        clean_string("contract_type").alias("contract_type"),
        F.upper(clean_string("status")).alias("status"),
        F.to_date(clean_string("effective_start_date")).alias("effective_start_date"),
        F.to_date(clean_string("effective_end_date")).alias("effective_end_date"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_nominations = add_silver_metadata(
    silver_nominations,
    f"{BRONZE_SCHEMA}.bronze_nominations_daily",
)

silver_tickets = (
    bronze_tickets
    .select(
        clean_string("ticket_id").alias("ticket_id"),
        F.to_timestamp(clean_string("opened_datetime")).alias("opened_datetime"),
        F.to_timestamp(clean_string("closed_datetime")).alias("closed_datetime"),
        clean_string("system_name").alias("system_name"),
        clean_string("issue_type").alias("issue_type"),
        clean_string("severity").alias("severity"),
        clean_string("facility_id").alias("facility_id"),
        clean_string("meter_id").alias("meter_id"),
        clean_string("reported_by_team").alias("reported_by_team"),
        clean_string("ticket_status").alias("ticket_status"),
        clean_string("business_impact").alias("business_impact"),
        clean_string("root_cause_category").alias("root_cause_category"),
        clean_string("resolution_summary").alias("resolution_summary"),
        clean_string("related_batch_id").alias("related_batch_id"),
        clean_string("related_exception_code").alias("related_exception_code"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_tickets = add_silver_metadata(
    silver_tickets,
    f"{BRONZE_SCHEMA}.bronze_support_tickets",
)

silver_pipeline = (
    bronze_pipeline
    .select(
        clean_string("run_id").alias("run_id"),
        clean_string("pipeline_name").alias("pipeline_name"),
        clean_string("source_file").alias("source_file"),
        F.to_timestamp(clean_string("started_at")).alias("started_at"),
        F.to_timestamp(clean_string("completed_at")).alias("completed_at"),
        F.upper(clean_string("status")).alias("status"),
        F.col("rows_read").cast("long").alias("rows_read"),
        F.col("rows_accepted").cast("long").alias("rows_accepted"),
        F.col("rows_rejected").cast("long").alias("rows_rejected"),
        F.col("error_count").cast("long").alias("error_count"),
        F.col("warning_count").cast("long").alias("warning_count"),
        clean_string("error_message").alias("error_message"),
        clean_string("trigger_type").alias("trigger_type"),
        clean_string("environment").alias("environment"),
        F.col("_source_file").alias("_bronze_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_pipeline = add_silver_metadata(
    silver_pipeline,
    f"{BRONZE_SCHEMA}.bronze_pipeline_run_log",
)

silver_dq_rules = (
    bronze_dq_rules
    .select(
        clean_string("rule_id").alias("rule_id"),
        clean_string("rule_name").alias("rule_name"),
        clean_string("layer").alias("layer"),
        clean_string("field_name").alias("field_name"),
        clean_string("rule_type").alias("rule_type"),
        clean_string("condition_description").alias("condition_description"),
        clean_string("severity").alias("severity"),
        clean_string("business_reason").alias("business_reason"),
        clean_string("owner_team").alias("owner_team"),
        F.upper(clean_string("active_flag")).alias("active_flag"),
        yes_no_to_boolean("active_flag").alias("is_active"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_dq_rules = add_silver_metadata(
    silver_dq_rules,
    f"{BRONZE_SCHEMA}.bronze_dq_rules_reference",
)

silver_scenarios = (
    bronze_scenarios
    .select(
        clean_string("scenario_id").alias("scenario_id"),
        clean_string("scenario_name").alias("scenario_name"),
        clean_string("description").alias("description"),
        clean_string("affected_facility_id").alias("affected_facility_id"),
        clean_string("affected_meter_ids").alias("affected_meter_ids"),
        F.to_timestamp(clean_string("start_datetime")).alias("start_datetime"),
        F.to_timestamp(clean_string("end_datetime")).alias("end_datetime"),
        clean_string("expected_exception_type").alias("expected_exception_type"),
        clean_string("expected_root_cause").alias("expected_root_cause"),
        clean_string("recommended_next_step").alias("recommended_next_step"),
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_record_hash").alias("_bronze_record_hash"),
    )
)
silver_scenarios = add_silver_metadata(
    silver_scenarios,
    f"{BRONZE_SCHEMA}.bronze_known_issue_scenarios",
)

# -----------------------------------------------------------------------------
# Build duplicate-grain helper
# -----------------------------------------------------------------------------

duplicate_counts = (
    silver_raw_readings
    .groupBy("meter_id", "reading_timestamp")
    .agg(F.count("*").alias("duplicate_meter_timestamp_count"))
)

# -----------------------------------------------------------------------------
# Prepare join helper tables
# -----------------------------------------------------------------------------

meter_dim = (
    silver_meter
    .select(
        F.col("meter_id").alias("master_meter_id"),
        F.col("facility_id").alias("master_facility_id"),
        F.col("meter_name").alias("master_meter_name"),
        F.col("meter_type").alias("master_meter_type"),
        F.col("product").alias("master_product"),
        F.col("measurement_type").alias("master_measurement_type"),
        F.col("custody_transfer_flag").alias("master_custody_transfer_flag"),
        F.col("is_custody_transfer").alias("master_is_custody_transfer"),
        F.col("source_system").alias("master_source_system"),
        F.col("polling_platform").alias("master_polling_platform"),
        F.col("install_date").alias("master_install_date"),
        F.col("active_flag").alias("master_meter_active_flag"),
        F.col("is_active").alias("master_meter_is_active"),
        F.col("expected_min_volume").alias("master_expected_min_volume"),
        F.col("expected_max_volume").alias("master_expected_max_volume"),
        F.col("sample_interval_minutes").alias("master_sample_interval_minutes"),
    )
)

facility_dim = (
    silver_facility
    .select(
        F.col("facility_id").alias("facility_dim_id"),
        F.col("facility_name").alias("master_facility_name"),
        F.col("region").alias("master_region"),
        F.col("basin").alias("master_basin"),
        F.col("state").alias("master_state"),
        F.col("asset_type").alias("master_asset_type"),
        F.col("operator").alias("master_operator"),
        F.col("active_flag").alias("master_facility_active_flag"),
        F.col("is_active").alias("master_facility_is_active"),
        F.col("latitude").alias("master_latitude"),
        F.col("longitude").alias("master_longitude"),
    )
)

mongo_context = (
    silver_mongo_events
    .select(
        F.col("raw_reading_id").alias("mongo_raw_reading_id"),
        F.col("event_id").alias("mongo_event_id"),
        F.col("payload_quality_code").alias("mongo_quality_code"),
        F.col("payload_raw_status").alias("mongo_raw_status"),
        F.col("payload_device_status").alias("mongo_device_status"),
        F.col("payload_communication_status").alias("mongo_communication_status"),
        F.col("payload_battery_status").alias("mongo_battery_status"),
        F.col("payload_polling_error_code").alias("mongo_polling_error_code"),
        F.col("payload_signal_quality").alias("mongo_signal_quality"),
        F.col("payload_retry_count").alias("mongo_retry_count"),
        F.col("payload_schema_version").alias("mongo_payload_schema_version"),
        F.col("seed_batch_id").alias("mongo_seed_batch_id"),
        F.col("scenario_id").alias("mongo_scenario_id"),
        F.col("payload_json").alias("mongo_payload_json"),
        F.col("raw_document_json").alias("mongo_raw_document_json"),
    )
)

# -----------------------------------------------------------------------------
# Build enriched Silver meter reading table
# -----------------------------------------------------------------------------

enriched = (
    silver_raw_readings.alias("raw")
    .join(
        meter_dim.alias("meter"),
        F.col("raw.meter_id") == F.col("meter.master_meter_id"),
        "left",
    )
    .join(
        facility_dim.alias("facility"),
        F.col("meter.master_facility_id") == F.col("facility.facility_dim_id"),
        "left",
    )
    .join(
        duplicate_counts.alias("dupes"),
        (F.col("raw.meter_id") == F.col("dupes.meter_id"))
        & (F.col("raw.reading_timestamp") == F.col("dupes.reading_timestamp")),
        "left",
    )
    .join(
        mongo_context.alias("mongo"),
        F.col("raw.raw_reading_id") == F.col("mongo.mongo_raw_reading_id"),
        "left",
    )
    .select(
        F.col("raw.raw_reading_id"),
        F.col("raw.meter_id"),
        F.col("meter.master_meter_id"),
        F.col("raw.source_facility_id"),
        F.col("meter.master_facility_id"),
        F.col("facility.master_facility_name"),
        F.col("facility.master_region"),
        F.col("facility.master_basin"),
        F.col("facility.master_state"),
        F.col("facility.master_asset_type"),
        F.col("facility.master_operator"),
        F.col("facility.master_facility_active_flag"),
        F.col("facility.master_facility_is_active"),
        F.col("raw.reading_timestamp"),
        F.col("raw.production_date"),
        F.col("raw.gas_day"),
        F.col("raw.poll_timestamp"),
        F.col("raw.load_timestamp"),
        F.col("raw.source_system"),
        F.col("raw.polling_platform"),
        F.col("raw.volume"),
        F.col("raw.pressure"),
        F.col("raw.temperature"),
        F.col("raw.quality_code"),
        F.col("raw.raw_status"),
        F.col("raw.batch_id"),
        F.col("meter.master_meter_name"),
        F.col("meter.master_meter_type"),
        F.col("meter.master_product"),
        F.col("meter.master_measurement_type"),
        F.col("meter.master_custody_transfer_flag"),
        F.col("meter.master_is_custody_transfer"),
        F.col("meter.master_source_system"),
        F.col("meter.master_polling_platform"),
        F.col("meter.master_install_date"),
        F.col("meter.master_meter_active_flag"),
        F.col("meter.master_meter_is_active"),
        F.col("meter.master_expected_min_volume"),
        F.col("meter.master_expected_max_volume"),
        F.col("meter.master_sample_interval_minutes"),
        F.coalesce(F.col("dupes.duplicate_meter_timestamp_count"), F.lit(1)).alias("duplicate_meter_timestamp_count"),
        F.col("mongo.mongo_event_id"),
        F.col("mongo.mongo_quality_code"),
        F.col("mongo.mongo_raw_status"),
        F.col("mongo.mongo_device_status"),
        F.col("mongo.mongo_communication_status"),
        F.col("mongo.mongo_battery_status"),
        F.col("mongo.mongo_polling_error_code"),
        F.col("mongo.mongo_signal_quality"),
        F.col("mongo.mongo_retry_count"),
        F.col("mongo.mongo_payload_schema_version"),
        F.col("mongo.mongo_seed_batch_id"),
        F.col("mongo.mongo_scenario_id"),
        F.col("mongo.mongo_payload_json"),
        F.col("mongo.mongo_raw_document_json"),
        F.col("raw._bronze_run_id"),
        F.col("raw._source_file"),
        F.col("raw._source_path"),
        F.col("raw._source_type"),
        F.col("raw._bronze_ingested_at"),
        F.col("raw._bronze_record_hash"),
    )
)

# -----------------------------------------------------------------------------
# Apply Silver DQ flags
# -----------------------------------------------------------------------------

enriched = (
    enriched
    .withColumn("dq_null_volume", F.col("volume").isNull())
    .withColumn(
        "dq_zero_volume",
        (F.col("volume") == 0)
        & F.coalesce(F.col("master_meter_is_active"), F.lit(False)),
    )
    .withColumn("dq_negative_volume", F.col("volume") < 0)
    .withColumn("dq_future_date", F.col("production_date") > F.current_date())
    .withColumn("dq_invalid_meter", F.col("master_meter_id").isNull())
    .withColumn(
        "dq_inactive_meter_reporting",
        F.col("master_meter_id").isNotNull()
        & F.coalesce(F.col("master_meter_is_active") == F.lit(False), F.lit(False)),
    )
    .withColumn(
        "dq_facility_mismatch",
        F.col("master_facility_id").isNotNull()
        & F.col("source_facility_id").isNotNull()
        & (F.col("source_facility_id") != F.col("master_facility_id")),
    )
    .withColumn(
        "dq_invalid_quality_code",
        ~F.coalesce(
            F.col("quality_code").isin(*ACCEPTED_QUALITY_CODES),
            F.lit(False),
        ),
    )
    .withColumn(
        "dq_late_arrival",
        F.col("load_timestamp").isNotNull()
        & F.col("reading_timestamp").isNotNull()
        & (F.col("load_timestamp") > F.expr("reading_timestamp + INTERVAL 24 HOURS")),
    )
    .withColumn(
        "dq_stale_reading",
        bool_or_false(F.col("quality_code") == "STALE")
        | bool_or_false(F.col("raw_status") == "STALE")
        | bool_or_false(F.col("mongo_quality_code") == "STALE")
        | bool_or_false(F.col("mongo_raw_status") == "STALE"),
    )
    .withColumn(
        "dq_duplicate_meter_timestamp",
        F.col("duplicate_meter_timestamp_count") > 1,
    )
)

dq_flag_columns = [
    "dq_null_volume",
    "dq_zero_volume",
    "dq_negative_volume",
    "dq_future_date",
    "dq_invalid_meter",
    "dq_inactive_meter_reporting",
    "dq_facility_mismatch",
    "dq_invalid_quality_code",
    "dq_late_arrival",
    "dq_stale_reading",
    "dq_duplicate_meter_timestamp",
]

dq_exception_count_expr = reduce(
    lambda left, right: left + right,
    [F.coalesce(F.col(col_name), F.lit(False)).cast("int") for col_name in dq_flag_columns],
)

dq_has_exception_expr = reduce(
    lambda left, right: left | right,
    [F.coalesce(F.col(col_name), F.lit(False)) for col_name in dq_flag_columns],
)

review_condition = (
    bool_or_false(F.col("quality_code") != "GOOD")
    | bool_or_false(F.col("raw_status") != "VALID")
    | bool_or_false(F.col("mongo_device_status").isin("NO_SIGNAL", "OFFLINE", "FAILED"))
    | bool_or_false(F.col("mongo_communication_status").isin("FAILED", "DEGRADED"))
)

enriched = (
    enriched
    .withColumn("dq_exception_count", dq_exception_count_expr)
    .withColumn("dq_has_exception", dq_has_exception_expr)
    .withColumn(
        "primary_exception_type",
        F.when(F.col("dq_invalid_meter"), F.lit("INVALID_METER"))
        .when(F.col("dq_duplicate_meter_timestamp"), F.lit("DUPLICATE_READING"))
        .when(F.col("dq_null_volume"), F.lit("NULL_VOLUME"))
        .when(F.col("dq_negative_volume"), F.lit("NEGATIVE_VOLUME"))
        .when(F.col("dq_zero_volume"), F.lit("ZERO_VOLUME"))
        .when(F.col("dq_future_date"), F.lit("FUTURE_DATE"))
        .when(F.col("dq_facility_mismatch"), F.lit("FACILITY_MISMATCH"))
        .when(F.col("dq_invalid_quality_code"), F.lit("INVALID_QUALITY_CODE"))
        .when(F.col("dq_late_arrival"), F.lit("LATE_ARRIVAL"))
        .when(F.col("dq_stale_reading"), F.lit("STALE_READING"))
        .when(F.col("dq_inactive_meter_reporting"), F.lit("INACTIVE_METER_REPORTING"))
        .otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "record_quality_status",
        F.when(F.col("dq_has_exception"), F.lit("EXCEPTION"))
        .when(review_condition, F.lit("REVIEW"))
        .otherwise(F.lit("VALID")),
    )
    .withColumn("_silver_run_id", F.lit(SILVER_RUN_ID))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_silver_record_hash", F.sha2(F.to_json(F.struct("*")), 256))
)

silver_meter_reading_enriched = enriched

silver_meter_reading_valid = (
    silver_meter_reading_enriched
    .filter(F.col("record_quality_status").isin("VALID", "REVIEW"))
)

silver_meter_reading_exceptions = (
    silver_meter_reading_enriched
    .filter(F.col("dq_has_exception"))
)

# -----------------------------------------------------------------------------
# Build row-level exception detail table
# -----------------------------------------------------------------------------

def exception_frame(rule_id: str, exception_type: str, severity: str, flag_column: str, business_reason: str):
    return (
        silver_meter_reading_enriched
        .filter(F.coalesce(F.col(flag_column), F.lit(False)))
        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.lit(rule_id),
                    F.col("raw_reading_id"),
                    F.col("meter_id"),
                    F.col("reading_timestamp").cast("string"),
                ),
                256,
            ).alias("exception_id"),
            F.lit(rule_id).alias("rule_id"),
            F.lit(exception_type).alias("exception_type"),
            F.lit(severity).alias("severity"),
            F.lit(business_reason).alias("business_reason"),
            F.col("raw_reading_id"),
            F.col("meter_id"),
            F.col("source_facility_id"),
            F.col("master_facility_id"),
            F.col("master_facility_name"),
            F.col("master_region"),
            F.col("master_basin"),
            F.col("reading_timestamp"),
            F.col("production_date"),
            F.col("poll_timestamp"),
            F.col("load_timestamp"),
            F.col("source_system"),
            F.col("polling_platform"),
            F.col("batch_id"),
            F.col("volume"),
            F.col("quality_code"),
            F.col("raw_status"),
            F.col("mongo_event_id"),
            F.col("mongo_device_status"),
            F.col("mongo_communication_status"),
            F.col("mongo_battery_status"),
            F.col("mongo_signal_quality"),
            F.col("mongo_scenario_id"),
            F.col("primary_exception_type"),
            F.col("_silver_run_id"),
            F.current_timestamp().alias("detected_at"),
        )
    )

exception_frames = [
    exception_frame(
        "DQ_NULL_VOLUME",
        "NULL_VOLUME",
        "High",
        "dq_null_volume",
        "Missing volume can undercount or break downstream reporting.",
    ),
    exception_frame(
        "DQ_ZERO_VOLUME",
        "ZERO_VOLUME",
        "Medium/High",
        "dq_zero_volume",
        "Zero volume on an active meter may be real, but requires source/polling review.",
    ),
    exception_frame(
        "DQ_VOLUME_NON_NEGATIVE",
        "NEGATIVE_VOLUME",
        "High",
        "dq_negative_volume",
        "Negative volume is invalid for this measurement dataset.",
    ),
    exception_frame(
        "DQ_PRODUCTION_DATE_NOT_FUTURE",
        "FUTURE_DATE",
        "Medium",
        "dq_future_date",
        "Future production dates indicate source/date mapping issues.",
    ),
    exception_frame(
        "DQ_METER_MUST_EXIST_IN_MASTER",
        "INVALID_METER",
        "High",
        "dq_invalid_meter",
        "Unmapped meters can disappear from reports or break joins.",
    ),
    exception_frame(
        "DQ_ACTIVE_METER_ONLY",
        "INACTIVE_METER_REPORTING",
        "Medium/High",
        "dq_inactive_meter_reporting",
        "Inactive meters reporting data require effective-date or business-rule review.",
    ),
    exception_frame(
        "DQ_FACILITY_MUST_MATCH_MASTER",
        "FACILITY_MISMATCH",
        "High",
        "dq_facility_mismatch",
        "Facility mismatch can move volumes to the wrong asset.",
    ),
    exception_frame(
        "DQ_QUALITY_CODE_VALID",
        "INVALID_QUALITY_CODE",
        "Medium",
        "dq_invalid_quality_code",
        "Unexpected quality/status codes can break rule interpretation.",
    ),
    exception_frame(
        "DQ_LATE_ARRIVAL",
        "LATE_ARRIVAL",
        "Medium",
        "dq_late_arrival",
        "Late-arriving data can cause reports to be wrong at refresh time.",
    ),
    exception_frame(
        "DQ_STALE_READING",
        "STALE_READING",
        "Medium/High",
        "dq_stale_reading",
        "Stale readings can look valid unless freshness and source context are checked.",
    ),
    exception_frame(
        "DQ_DUPLICATE_METER_TIMESTAMP",
        "DUPLICATE_READING",
        "High",
        "dq_duplicate_meter_timestamp",
        "Duplicate meter/timestamp records can inflate totals.",
    ),
]

silver_data_quality_exceptions = reduce(
    lambda left, right: left.unionByName(right),
    exception_frames,
)

silver_quality_rule_summary = (
    silver_data_quality_exceptions
    .groupBy("rule_id", "exception_type", "severity")
    .agg(
        F.count("*").alias("exception_count"),
        F.countDistinct("meter_id").alias("affected_meter_count"),
        F.countDistinct("master_facility_id").alias("affected_facility_count"),
        F.min("production_date").alias("first_production_date"),
        F.max("production_date").alias("last_production_date"),
    )
    .orderBy(F.desc("exception_count"))
)

# -----------------------------------------------------------------------------
# Reconciliation / status summary
# -----------------------------------------------------------------------------

silver_reconciliation_counts = spark.createDataFrame(
    [
        {
            "layer": "bronze",
            "object_name": "bronze_raw_polling_readings",
            "row_count": bronze_raw.count(),
            "notes": "Structured raw readings from CSV source package.",
        },
        {
            "layer": "bronze",
            "object_name": "bronze_meter_polling_events",
            "row_count": bronze_mongo.count(),
            "notes": "Raw MongoDB Atlas event documents.",
        },
        {
            "layer": "silver",
            "object_name": "silver_meter_reading_enriched",
            "row_count": silver_meter_reading_enriched.count(),
            "notes": "Standardized readings joined to master data and MongoDB context.",
        },
        {
            "layer": "silver",
            "object_name": "silver_meter_reading_valid",
            "row_count": silver_meter_reading_valid.count(),
            "notes": "Records with status VALID or REVIEW after Silver checks.",
        },
        {
            "layer": "silver",
            "object_name": "silver_meter_reading_exceptions",
            "row_count": silver_meter_reading_exceptions.count(),
            "notes": "Records with one or more DQ exceptions.",
        },
        {
            "layer": "silver",
            "object_name": "silver_data_quality_exceptions",
            "row_count": silver_data_quality_exceptions.count(),
            "notes": "One row per failed rule per affected reading.",
        },
    ]
)

# -----------------------------------------------------------------------------
# Write Silver tables
# -----------------------------------------------------------------------------

print("=" * 90)
print("Writing Silver tables...")

write_summary = []

tables_to_write = [
    ("silver_facility_master", silver_facility, 25),
    ("silver_meter_master", silver_meter, 125),
    ("silver_raw_polling_readings", silver_raw_readings, 180_056),
    ("silver_meter_polling_events", silver_mongo_events, 25_343),
    ("silver_flowcal_measurement_extract", silver_flowcal, 7_500),
    ("silver_nominations_daily", silver_nominations, 1_500),
    ("silver_support_tickets", silver_tickets, 350),
    ("silver_pipeline_run_log", silver_pipeline, 150),
    ("silver_dq_rules_reference", silver_dq_rules, 20),
    ("silver_known_issue_scenarios", silver_scenarios, 15),
    ("silver_meter_reading_enriched", silver_meter_reading_enriched, 180_056),
    ("silver_meter_reading_valid", silver_meter_reading_valid, None),
    ("silver_meter_reading_exceptions", silver_meter_reading_exceptions, None),
    ("silver_data_quality_exceptions", silver_data_quality_exceptions, None),
    ("silver_quality_rule_summary", silver_quality_rule_summary, None),
    ("silver_reconciliation_counts", silver_reconciliation_counts, None),
]

for table_name, df, expected_rows in tables_to_write:
    written_rows = write_silver_table(df, table_name)
    if expected_rows is None:
        status = "CREATED"
    elif written_rows == expected_rows:
        status = "PASS"
    else:
        status = "CHECK"
    write_summary.append(
        {
            "table_name": f"{SILVER_SCHEMA}.{table_name}",
            "expected_rows": expected_rows,
            "written_rows": written_rows,
            "status": status,
        }
    )

# -----------------------------------------------------------------------------
# Display summary and validation outputs
# -----------------------------------------------------------------------------

summary_df = spark.createDataFrame(write_summary)

print("=" * 90)
print("Silver quality rules complete.")
print(f"Silver schema/database: {SILVER_SCHEMA}")
print(f"Silver run ID: {SILVER_RUN_ID}")

display(summary_df.orderBy("table_name"))

failed_df = summary_df.filter(F.col("status") == "CHECK")
failed_count = failed_df.count()

if failed_count > 0:
    print("WARNING: One or more Silver tables had unexpected row counts.")
    display(failed_df)
else:
    print("Silver fixed-count validation checks passed.")

print("=" * 90)
print("Record quality status counts:")
display(
    spark.table(f"{SILVER_SCHEMA}.silver_meter_reading_enriched")
    .groupBy("record_quality_status")
    .count()
    .orderBy("record_quality_status")
)

print("=" * 90)
print("Primary exception type counts:")
display(
    spark.table(f"{SILVER_SCHEMA}.silver_meter_reading_exceptions")
    .groupBy("primary_exception_type")
    .count()
    .orderBy(F.desc("count"))
)

print("=" * 90)
print("DQ rule summary:")
display(
    spark.table(f"{SILVER_SCHEMA}.silver_quality_rule_summary")
)

print("=" * 90)
print("Silver reconciliation counts:")
display(
    spark.table(f"{SILVER_SCHEMA}.silver_reconciliation_counts")
)

print("=" * 90)
print("Silver tables:")
display(spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}"))