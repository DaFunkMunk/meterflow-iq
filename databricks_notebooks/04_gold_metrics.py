"""
MeterFlow IQ - 04 Gold Metrics

Purpose:
Create business-ready Gold tables from Silver outputs.

Gold responsibilities:
- Create business dimensions for facilities and meters
- Create meter-day and facility-day measurement facts
- Create actual-vs-nominated business KPI facts
- Create FlowCal reconciliation facts
- Create data-quality exception facts and summaries
- Create pipeline health and support ticket facts
- Create source-to-target reconciliation outputs
- Create RCA context for Streamlit / AI helper use

Gold rule:
Gold is business-ready and consumption-ready. It should be usable by the
Publisher / Warehouse Layer for BigQuery, Snowflake, and Azure SQL.
"""

import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

SILVER_SCHEMA = "meterflow_iq_silver"
BRONZE_SCHEMA = "meterflow_iq_bronze"
GOLD_SCHEMA = "meterflow_iq_gold"
GOLD_RUN_ID = f"gold_metrics_{uuid.uuid4().hex[:12]}"

print("Spark version:", spark.version)
print("Gold run ID:", GOLD_RUN_ID)
print("Silver schema:", SILVER_SCHEMA)
print("Gold schema:", GOLD_SCHEMA)


# -----------------------------------------------------------------------------
# Create Gold schema/database
# -----------------------------------------------------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS " + GOLD_SCHEMA)
spark.sql("USE " + GOLD_SCHEMA)


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def table(full_table_name: str):
    """
    Small wrapper for reading a Spark table.
    """
    return spark.table(full_table_name)


def safe_divide(numerator, denominator):
    """
    Safely divide two Spark column expressions.
    Returns NULL when the denominator is NULL or zero.
    """
    return (
        F.when(
            F.coalesce(denominator.cast("double"), F.lit(0.0)) == F.lit(0.0),
            F.lit(None).cast("double"),
        )
        .otherwise(numerator.cast("double") / denominator.cast("double"))
    )


def sum_bool(column_name: str):
    """
    Sum boolean flags as integer counts.
    """
    return F.sum(F.coalesce(F.col(column_name), F.lit(False)).cast("int"))


def write_gold_table(df, table_name: str) -> int:
    """
    Write a Gold table and return the written row count.
    """
    full_table_name = f"{GOLD_SCHEMA}.{table_name}"

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


def add_gold_metadata(df, source_description: str):
    """
    Add standard Gold processing metadata.
    """
    return (
        df
        .withColumn("_gold_run_id", F.lit(GOLD_RUN_ID))
        .withColumn("_gold_source", F.lit(source_description))
        .withColumn("_gold_processed_at", F.current_timestamp())
        .withColumn("_gold_record_hash", F.sha2(F.to_json(F.struct("*")), 256))
    )


# -----------------------------------------------------------------------------
# Read Silver tables
# -----------------------------------------------------------------------------

print("=" * 90)
print("Reading Silver tables...")

silver_facility = table(f"{SILVER_SCHEMA}.silver_facility_master")
silver_meter = table(f"{SILVER_SCHEMA}.silver_meter_master")
silver_readings = table(f"{SILVER_SCHEMA}.silver_meter_reading_enriched")
silver_dq_exceptions = table(f"{SILVER_SCHEMA}.silver_data_quality_exceptions")
silver_quality_rule_summary = table(f"{SILVER_SCHEMA}.silver_quality_rule_summary")
silver_pipeline = table(f"{SILVER_SCHEMA}.silver_pipeline_run_log")
silver_tickets = table(f"{SILVER_SCHEMA}.silver_support_tickets")
silver_nominations = table(f"{SILVER_SCHEMA}.silver_nominations_daily")
silver_flowcal = table(f"{SILVER_SCHEMA}.silver_flowcal_measurement_extract")
silver_scenarios = table(f"{SILVER_SCHEMA}.silver_known_issue_scenarios")
silver_reconciliation = table(f"{SILVER_SCHEMA}.silver_reconciliation_counts")

print("Silver tables loaded.")


# -----------------------------------------------------------------------------
# Gold dimensions
# -----------------------------------------------------------------------------

dim_facility = (
    silver_facility
    .select(
        F.sha2(F.col("facility_id"), 256).alias("facility_key"),
        F.col("facility_id"),
        F.col("facility_name"),
        F.col("region"),
        F.col("basin"),
        F.col("state"),
        F.col("asset_type"),
        F.col("operator"),
        F.col("active_flag"),
        F.col("is_active"),
        F.col("effective_start_date"),
        F.col("effective_end_date"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("source_system"),
    )
)

dim_facility = add_gold_metadata(
    dim_facility,
    f"{SILVER_SCHEMA}.silver_facility_master",
)

dim_meter = (
    silver_meter.alias("m")
    .join(
        silver_facility.alias("f"),
        F.col("m.facility_id") == F.col("f.facility_id"),
        "left",
    )
    .select(
        F.sha2(F.col("m.meter_id"), 256).alias("meter_key"),
        F.sha2(F.col("m.facility_id"), 256).alias("facility_key"),
        F.col("m.meter_id"),
        F.col("m.facility_id"),
        F.col("f.facility_name"),
        F.col("f.region"),
        F.col("f.basin"),
        F.col("f.state"),
        F.col("f.asset_type"),
        F.col("f.operator"),
        F.col("m.meter_name"),
        F.col("m.meter_type"),
        F.col("m.product"),
        F.col("m.measurement_type"),
        F.col("m.custody_transfer_flag"),
        F.col("m.is_custody_transfer"),
        F.col("m.source_system"),
        F.col("m.polling_platform"),
        F.col("m.install_date"),
        F.col("m.active_flag"),
        F.col("m.is_active"),
        F.col("m.expected_min_volume"),
        F.col("m.expected_max_volume"),
        F.col("m.sample_interval_minutes"),
    )
)

dim_meter = add_gold_metadata(
    dim_meter,
    f"{SILVER_SCHEMA}.silver_meter_master + {SILVER_SCHEMA}.silver_facility_master",
)


# -----------------------------------------------------------------------------
# Gold base readings
# -----------------------------------------------------------------------------

readings_gold_base = (
    silver_readings
    .withColumn(
        "reporting_facility_id",
        F.coalesce(F.col("master_facility_id"), F.col("source_facility_id")),
    )
    .withColumn(
        "is_valid_record",
        F.col("record_quality_status") == F.lit("VALID"),
    )
    .withColumn(
        "is_review_record",
        F.col("record_quality_status") == F.lit("REVIEW"),
    )
    .withColumn(
        "is_exception_record",
        F.col("record_quality_status") == F.lit("EXCEPTION"),
    )
    .withColumn(
        "valid_volume",
        F.when(
            (F.col("record_quality_status") == "VALID")
            & F.col("volume").isNotNull()
            & (F.col("volume") >= 0),
            F.col("volume"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "accepted_or_review_volume",
        F.when(
            F.col("record_quality_status").isin("VALID", "REVIEW")
            & F.col("volume").isNotNull()
            & (F.col("volume") >= 0),
            F.col("volume"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "all_nonnegative_volume",
        F.when(
            F.col("volume").isNotNull() & (F.col("volume") >= 0),
            F.col("volume"),
        ).otherwise(F.lit(0.0)),
    )
)


# -----------------------------------------------------------------------------
# Gold fact: meter reading daily
# -----------------------------------------------------------------------------

fact_meter_reading_daily = (
    readings_gold_base
    .groupBy(
        "meter_id",
        "master_meter_id",
        "reporting_facility_id",
        "source_facility_id",
        "master_facility_id",
        "master_facility_name",
        "master_region",
        "master_basin",
        "master_state",
        "master_asset_type",
        "master_operator",
        "master_product",
        "master_meter_type",
        "master_measurement_type",
        "master_is_custody_transfer",
        "production_date",
        "gas_day",
        "source_system",
        "polling_platform",
    )
    .agg(
        F.count("*").alias("hourly_reading_count"),
        F.sum(F.col("is_valid_record").cast("int")).alias("valid_reading_count"),
        F.sum(F.col("is_review_record").cast("int")).alias("review_reading_count"),
        F.sum(F.col("is_exception_record").cast("int")).alias("exception_reading_count"),
        F.sum("valid_volume").alias("valid_volume"),
        F.sum("accepted_or_review_volume").alias("accepted_or_review_volume"),
        F.sum("all_nonnegative_volume").alias("all_nonnegative_volume"),
        F.sum(F.coalesce(F.col("volume"), F.lit(0.0))).alias("raw_volume_sum"),
        F.avg("pressure").alias("avg_pressure"),
        F.avg("temperature").alias("avg_temperature"),
        F.min("reading_timestamp").alias("first_reading_timestamp"),
        F.max("reading_timestamp").alias("last_reading_timestamp"),
        F.min("poll_timestamp").alias("first_poll_timestamp"),
        F.max("poll_timestamp").alias("last_poll_timestamp"),
        F.max("load_timestamp").alias("latest_load_timestamp"),
        F.countDistinct("batch_id").alias("batch_count"),
        F.countDistinct("raw_reading_id").alias("raw_reading_count"),
        sum_bool("dq_null_volume").alias("null_volume_count"),
        sum_bool("dq_zero_volume").alias("zero_volume_count"),
        sum_bool("dq_negative_volume").alias("negative_volume_count"),
        sum_bool("dq_future_date").alias("future_date_count"),
        sum_bool("dq_invalid_meter").alias("invalid_meter_count"),
        sum_bool("dq_inactive_meter_reporting").alias("inactive_meter_reporting_count"),
        sum_bool("dq_facility_mismatch").alias("facility_mismatch_count"),
        sum_bool("dq_invalid_quality_code").alias("invalid_quality_code_count"),
        sum_bool("dq_late_arrival").alias("late_arrival_count"),
        sum_bool("dq_stale_reading").alias("stale_reading_count"),
        sum_bool("dq_duplicate_meter_timestamp").alias("duplicate_meter_timestamp_count"),
        F.max("dq_exception_count").alias("max_exception_count_on_reading"),
        F.collect_set("primary_exception_type").alias("exception_types"),
        F.collect_set("quality_code").alias("quality_codes"),
        F.collect_set("raw_status").alias("raw_statuses"),
        F.collect_set("mongo_device_status").alias("mongo_device_statuses"),
        F.collect_set("mongo_communication_status").alias("mongo_communication_statuses"),
        F.collect_set("mongo_signal_quality").alias("mongo_signal_qualities"),
    )
    .withColumn(
        "exception_rate",
        safe_divide(F.col("exception_reading_count"), F.col("hourly_reading_count")),
    )
    .withColumn(
        "valid_record_rate",
        safe_divide(F.col("valid_reading_count"), F.col("hourly_reading_count")),
    )
    .withColumn(
        "reporting_readiness_score",
        F.round(
            (F.lit(1.0) - F.coalesce(F.col("exception_rate"), F.lit(0.0))) * 100.0,
            2,
        ),
    )
    .withColumn(
        "has_data_quality_issue",
        F.col("exception_reading_count") > 0,
    )
)

fact_meter_reading_daily = add_gold_metadata(
    fact_meter_reading_daily,
    f"{SILVER_SCHEMA}.silver_meter_reading_enriched",
)


# -----------------------------------------------------------------------------
# Gold fact: facility volume daily
# -----------------------------------------------------------------------------

fact_facility_volume_daily = (
    fact_meter_reading_daily
    .groupBy(
        "reporting_facility_id",
        "master_facility_name",
        "master_region",
        "master_basin",
        "master_state",
        "master_asset_type",
        "master_operator",
        "master_product",
        "production_date",
        "gas_day",
    )
    .agg(
        F.countDistinct("meter_id").alias("meter_count"),
        F.sum("hourly_reading_count").alias("hourly_reading_count"),
        F.sum("valid_reading_count").alias("valid_reading_count"),
        F.sum("review_reading_count").alias("review_reading_count"),
        F.sum("exception_reading_count").alias("exception_reading_count"),
        F.sum("valid_volume").alias("valid_volume"),
        F.sum("accepted_or_review_volume").alias("accepted_or_review_volume"),
        F.sum("all_nonnegative_volume").alias("all_nonnegative_volume"),
        F.sum("raw_volume_sum").alias("raw_volume_sum"),
        F.avg("avg_pressure").alias("avg_pressure"),
        F.avg("avg_temperature").alias("avg_temperature"),
        F.min("first_reading_timestamp").alias("first_reading_timestamp"),
        F.max("last_reading_timestamp").alias("last_reading_timestamp"),
        F.max("latest_load_timestamp").alias("latest_load_timestamp"),
        F.sum("null_volume_count").alias("null_volume_count"),
        F.sum("zero_volume_count").alias("zero_volume_count"),
        F.sum("negative_volume_count").alias("negative_volume_count"),
        F.sum("future_date_count").alias("future_date_count"),
        F.sum("invalid_meter_count").alias("invalid_meter_count"),
        F.sum("inactive_meter_reporting_count").alias("inactive_meter_reporting_count"),
        F.sum("facility_mismatch_count").alias("facility_mismatch_count"),
        F.sum("invalid_quality_code_count").alias("invalid_quality_code_count"),
        F.sum("late_arrival_count").alias("late_arrival_count"),
        F.sum("stale_reading_count").alias("stale_reading_count"),
        F.sum("duplicate_meter_timestamp_count").alias("duplicate_meter_timestamp_count"),
    )
    .withColumn(
        "exception_rate",
        safe_divide(F.col("exception_reading_count"), F.col("hourly_reading_count")),
    )
    .withColumn(
        "valid_record_rate",
        safe_divide(F.col("valid_reading_count"), F.col("hourly_reading_count")),
    )
    .withColumn(
        "reporting_readiness_score",
        F.round(
            (F.lit(1.0) - F.coalesce(F.col("exception_rate"), F.lit(0.0))) * 100.0,
            2,
        ),
    )
    .withColumn(
        "has_data_quality_issue",
        F.col("exception_reading_count") > 0,
    )
)

fact_facility_volume_daily = add_gold_metadata(
    fact_facility_volume_daily,
    "fact_meter_reading_daily",
)


# -----------------------------------------------------------------------------
# Gold fact: actual vs nominated daily
# -----------------------------------------------------------------------------

nomination_daily = (
    silver_nominations
    .groupBy(
        F.col("facility_id").alias("nomination_facility_id"),
        F.col("production_date").alias("nomination_production_date"),
        F.col("product").alias("nomination_product"),
    )
    .agg(
        F.sum("nominated_volume").alias("nominated_volume"),
        F.countDistinct("nomination_id").alias("nomination_count"),
        F.collect_set("customer_group").alias("customer_groups"),
        F.collect_set("contract_type").alias("contract_types"),
        F.collect_set("status").alias("nomination_statuses"),
    )
)

actual_daily = (
    fact_facility_volume_daily
    .select(
        F.col("reporting_facility_id"),
        F.col("production_date"),
        F.col("master_product"),
        F.col("master_facility_name"),
        F.col("master_region"),
        F.col("master_basin"),
        F.col("master_state"),
        F.col("accepted_or_review_volume").alias("actual_volume"),
        F.col("valid_volume"),
        F.col("exception_rate"),
        F.col("reporting_readiness_score"),
        F.col("has_data_quality_issue"),
    )
)

fact_actual_vs_nominated_daily = (
    actual_daily.alias("actual")
    .join(
        nomination_daily.alias("nom"),
        (F.col("actual.reporting_facility_id") == F.col("nom.nomination_facility_id"))
        & (F.col("actual.production_date") == F.col("nom.nomination_production_date"))
        & (F.col("actual.master_product") == F.col("nom.nomination_product")),
        "left",
    )
    .select(
        F.col("actual.reporting_facility_id").alias("facility_id"),
        F.col("actual.master_facility_name").alias("facility_name"),
        F.col("actual.master_region").alias("region"),
        F.col("actual.master_basin").alias("basin"),
        F.col("actual.master_state").alias("state"),
        F.col("actual.production_date"),
        F.col("actual.master_product").alias("product"),
        F.col("actual.actual_volume"),
        F.col("actual.valid_volume"),
        F.col("nom.nominated_volume"),
        (F.col("actual.actual_volume") - F.col("nom.nominated_volume")).alias(
            "actual_vs_nominated_delta"
        ),
        safe_divide(
            F.col("actual.actual_volume") - F.col("nom.nominated_volume"),
            F.col("nom.nominated_volume"),
        ).alias("actual_vs_nominated_pct"),
        F.col("nom.nomination_count"),
        F.col("nom.customer_groups"),
        F.col("nom.contract_types"),
        F.col("nom.nomination_statuses"),
        F.col("actual.exception_rate"),
        F.col("actual.reporting_readiness_score"),
        F.col("actual.has_data_quality_issue"),
    )
)

fact_actual_vs_nominated_daily = add_gold_metadata(
    fact_actual_vs_nominated_daily,
    f"{SILVER_SCHEMA}.silver_nominations_daily + fact_facility_volume_daily",
)


# -----------------------------------------------------------------------------
# Gold fact: FlowCal reconciliation daily
# -----------------------------------------------------------------------------

flowcal_daily = (
    silver_flowcal
    .groupBy(
        F.col("meter_id").alias("flowcal_meter_id"),
        F.col("facility_id").alias("flowcal_facility_id"),
        F.col("production_date").alias("flowcal_production_date"),
    )
    .agg(
        F.sum("measured_volume").alias("flowcal_measured_volume"),
        F.sum("corrected_volume").alias("flowcal_corrected_volume"),
        F.count("*").alias("flowcal_record_count"),
        F.sum(
            F.coalesce(
                F.col("is_approved") == F.lit(False),
                F.lit(False),
            ).cast("int")
        ).alias("unapproved_flowcal_count"),
        F.sum(
            F.coalesce(
                F.col("close_status") != F.lit("CLOSED"),
                F.lit(False),
            ).cast("int")
        ).alias("open_flowcal_count"),
        F.collect_set("validation_status").alias("flowcal_validation_statuses"),
        F.collect_set("exception_code").alias("flowcal_exception_codes"),
        F.max("last_updated_timestamp").alias("flowcal_last_updated_timestamp"),
    )
)

fact_flowcal_reconciliation_daily = (
    fact_meter_reading_daily.alias("meter")
    .join(
        flowcal_daily.alias("flowcal"),
        (F.col("meter.meter_id") == F.col("flowcal.flowcal_meter_id"))
        & (F.col("meter.production_date") == F.col("flowcal.flowcal_production_date")),
        "left",
    )
    .select(
        F.col("meter.meter_id"),
        F.col("meter.reporting_facility_id"),
        F.col("meter.master_facility_id"),
        F.col("meter.master_facility_name"),
        F.col("meter.master_region"),
        F.col("meter.master_basin"),
        F.col("meter.production_date"),
        F.col("meter.master_product").alias("product"),
        F.col("meter.accepted_or_review_volume").alias(
            "source_accepted_or_review_volume"
        ),
        F.col("meter.valid_volume").alias("source_valid_volume"),
        F.col("flowcal.flowcal_measured_volume"),
        F.col("flowcal.flowcal_corrected_volume"),
        (
            F.col("flowcal.flowcal_corrected_volume")
            - F.col("meter.accepted_or_review_volume")
        ).alias("corrected_vs_source_delta"),
        safe_divide(
            F.col("flowcal.flowcal_corrected_volume")
            - F.col("meter.accepted_or_review_volume"),
            F.col("meter.accepted_or_review_volume"),
        ).alias("corrected_vs_source_pct"),
        F.col("flowcal.flowcal_record_count"),
        F.col("flowcal.unapproved_flowcal_count"),
        F.col("flowcal.open_flowcal_count"),
        F.col("flowcal.flowcal_validation_statuses"),
        F.col("flowcal.flowcal_exception_codes"),
        F.col("flowcal.flowcal_last_updated_timestamp"),
        F.col("meter.exception_rate"),
        F.col("meter.reporting_readiness_score"),
    )
)

fact_flowcal_reconciliation_daily = add_gold_metadata(
    fact_flowcal_reconciliation_daily,
    f"{SILVER_SCHEMA}.silver_flowcal_measurement_extract + fact_meter_reading_daily",
)


# -----------------------------------------------------------------------------
# Gold fact: data quality exceptions
# -----------------------------------------------------------------------------

fact_data_quality_exception = (
    silver_dq_exceptions
    .select(
        F.col("exception_id"),
        F.col("rule_id"),
        F.col("exception_type"),
        F.col("severity"),
        F.col("business_reason"),
        F.col("raw_reading_id"),
        F.col("meter_id"),
        F.col("source_facility_id"),
        F.col("master_facility_id").alias("facility_id"),
        F.col("master_facility_name").alias("facility_name"),
        F.col("master_region").alias("region"),
        F.col("master_basin").alias("basin"),
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
        F.col("detected_at"),
    )
    .withColumn("exception_status", F.lit("New"))
    .withColumn("assigned_to", F.lit(None).cast("string"))
    .withColumn("analyst_notes", F.lit(None).cast("string"))
    .withColumn(
        "days_since_production_date",
        F.datediff(F.current_date(), F.col("production_date")),
    )
)

fact_data_quality_exception = add_gold_metadata(
    fact_data_quality_exception,
    f"{SILVER_SCHEMA}.silver_data_quality_exceptions",
)


# -----------------------------------------------------------------------------
# Gold fact: pipeline run
# -----------------------------------------------------------------------------

fact_pipeline_run = (
    silver_pipeline
    .select(
        F.col("run_id"),
        F.col("pipeline_name"),
        F.col("source_file"),
        F.col("started_at"),
        F.col("completed_at"),
        F.col("status"),
        F.col("rows_read"),
        F.col("rows_accepted"),
        F.col("rows_rejected"),
        F.col("error_count"),
        F.col("warning_count"),
        F.col("error_message"),
        F.col("trigger_type"),
        F.col("environment"),
    )
    .withColumn(
        "duration_seconds",
        F.unix_timestamp("completed_at") - F.unix_timestamp("started_at"),
    )
    .withColumn(
        "duration_minutes",
        F.round(F.col("duration_seconds") / F.lit(60.0), 2),
    )
    .withColumn(
        "accepted_rate",
        safe_divide(F.col("rows_accepted"), F.col("rows_read")),
    )
    .withColumn(
        "rejected_rate",
        safe_divide(F.col("rows_rejected"), F.col("rows_read")),
    )
    .withColumn(
        "is_success",
        F.col("status").isin("SUCCESS", "RETRY_SUCCESS"),
    )
    .withColumn(
        "is_failed",
        F.col("status") == F.lit("FAILED"),
    )
    .withColumn(
        "is_partial_load",
        F.col("status") == F.lit("PARTIAL_SUCCESS"),
    )
    .withColumn(
        "has_errors",
        F.coalesce(F.col("error_count"), F.lit(0)) > 0,
    )
)

fact_pipeline_run = add_gold_metadata(
    fact_pipeline_run,
    f"{SILVER_SCHEMA}.silver_pipeline_run_log",
)


# -----------------------------------------------------------------------------
# Gold fact: support ticket
# -----------------------------------------------------------------------------

fact_support_ticket = (
    silver_tickets
    .select(
        F.col("ticket_id"),
        F.col("opened_datetime"),
        F.col("closed_datetime"),
        F.col("system_name"),
        F.col("issue_type"),
        F.col("severity"),
        F.col("facility_id"),
        F.col("meter_id"),
        F.col("reported_by_team"),
        F.col("ticket_status"),
        F.col("business_impact"),
        F.col("root_cause_category"),
        F.col("resolution_summary"),
        F.col("related_batch_id"),
        F.col("related_exception_code"),
    )
    .withColumn(
        "is_open",
        F.col("closed_datetime").isNull(),
    )
    .withColumn(
        "ticket_age_days",
        F.datediff(
            F.coalesce(F.to_date(F.col("closed_datetime")), F.current_date()),
            F.to_date(F.col("opened_datetime")),
        ),
    )
    .withColumn(
        "is_high_severity",
        F.upper(F.col("severity")).isin("HIGH", "CRITICAL", "SEV1", "SEV2"),
    )
)

fact_support_ticket = add_gold_metadata(
    fact_support_ticket,
    f"{SILVER_SCHEMA}.silver_support_tickets",
)


# -----------------------------------------------------------------------------
# Gold exception summary daily
# -----------------------------------------------------------------------------

gold_exception_summary_daily = (
    fact_data_quality_exception
    .groupBy(
        "production_date",
        "facility_id",
        "facility_name",
        "region",
        "basin",
        "exception_type",
        "severity",
    )
    .agg(
        F.count("*").alias("exception_rule_failure_count"),
        F.countDistinct("exception_id").alias("exception_count"),
        F.countDistinct("raw_reading_id").alias("affected_reading_count"),
        F.countDistinct("meter_id").alias("affected_meter_count"),
        F.collect_set("source_system").alias("source_systems"),
        F.collect_set("polling_platform").alias("polling_platforms"),
        F.collect_set("mongo_device_status").alias("device_statuses"),
        F.collect_set("mongo_communication_status").alias("communication_statuses"),
        F.collect_set("mongo_signal_quality").alias("signal_qualities"),
        F.min("detected_at").alias("first_detected_at"),
        F.max("detected_at").alias("last_detected_at"),
    )
    .withColumn(
        "exception_group_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("production_date").cast("string"), F.lit("")),
                F.coalesce(F.col("facility_id"), F.lit("")),
                F.coalesce(F.col("exception_type"), F.lit("")),
                F.coalesce(F.col("severity"), F.lit("")),
            ),
            256,
        ),
    )
)

gold_exception_summary_daily = add_gold_metadata(
    gold_exception_summary_daily,
    "fact_data_quality_exception",
)


# -----------------------------------------------------------------------------
# Gold pipeline health summary
# -----------------------------------------------------------------------------

pipeline_rollup = (
    fact_pipeline_run
    .groupBy("pipeline_name")
    .agg(
        F.count("*").alias("run_count"),
        F.sum(F.col("is_success").cast("int")).alias("success_count"),
        F.sum(F.col("is_failed").cast("int")).alias("failed_count"),
        F.sum(F.col("is_partial_load").cast("int")).alias("partial_load_count"),
        F.sum(F.col("has_errors").cast("int")).alias("runs_with_errors_count"),
        F.max("started_at").alias("latest_started_at"),
        F.avg("duration_minutes").alias("avg_duration_minutes"),
        F.max("duration_minutes").alias("max_duration_minutes"),
        F.sum("rows_read").alias("total_rows_read"),
        F.sum("rows_accepted").alias("total_rows_accepted"),
        F.sum("rows_rejected").alias("total_rows_rejected"),
    )
)

latest_pipeline_window = (
    Window
    .partitionBy("pipeline_name")
    .orderBy(F.col("started_at").desc_nulls_last())
)

latest_pipeline_run = (
    fact_pipeline_run
    .withColumn("rn", F.row_number().over(latest_pipeline_window))
    .filter(F.col("rn") == 1)
    .select(
        F.col("pipeline_name"),
        F.col("run_id").alias("latest_run_id"),
        F.col("status").alias("latest_status"),
        F.col("started_at").alias("latest_run_started_at"),
        F.col("completed_at").alias("latest_run_completed_at"),
        F.col("duration_minutes").alias("latest_duration_minutes"),
        F.col("rows_read").alias("latest_rows_read"),
        F.col("rows_accepted").alias("latest_rows_accepted"),
        F.col("rows_rejected").alias("latest_rows_rejected"),
        F.col("error_message").alias("latest_error_message"),
    )
)

gold_pipeline_health_summary = (
    pipeline_rollup.alias("rollup")
    .join(
        latest_pipeline_run.alias("latest"),
        F.col("rollup.pipeline_name") == F.col("latest.pipeline_name"),
        "left",
    )
    .select(
        F.col("rollup.pipeline_name"),
        F.col("latest.latest_run_id"),
        F.col("latest.latest_status"),
        F.col("latest.latest_run_started_at"),
        F.col("latest.latest_run_completed_at"),
        F.col("latest.latest_duration_minutes"),
        F.col("latest.latest_rows_read"),
        F.col("latest.latest_rows_accepted"),
        F.col("latest.latest_rows_rejected"),
        F.col("latest.latest_error_message"),
        F.col("rollup.run_count"),
        F.col("rollup.success_count"),
        F.col("rollup.failed_count"),
        F.col("rollup.partial_load_count"),
        F.col("rollup.runs_with_errors_count"),
        F.col("rollup.avg_duration_minutes"),
        F.col("rollup.max_duration_minutes"),
        F.col("rollup.total_rows_read"),
        F.col("rollup.total_rows_accepted"),
        F.col("rollup.total_rows_rejected"),
        safe_divide(F.col("rollup.failed_count"), F.col("rollup.run_count")).alias("failure_rate"),
        safe_divide(
            F.col("rollup.total_rows_rejected"),
            F.col("rollup.total_rows_read"),
        ).alias("overall_rejected_rate"),
    )
)

gold_pipeline_health_summary = add_gold_metadata(
    gold_pipeline_health_summary,
    "fact_pipeline_run",
)


# -----------------------------------------------------------------------------
# Gold quality rule summary
# -----------------------------------------------------------------------------

gold_quality_rule_summary = (
    silver_quality_rule_summary
    .select(
        F.col("rule_id"),
        F.col("exception_type"),
        F.col("severity"),
        F.col("exception_count"),
        F.col("affected_meter_count"),
        F.col("affected_facility_count"),
        F.col("first_production_date"),
        F.col("last_production_date"),
    )
    .withColumn(
        "severity_rank",
        F.when(F.upper(F.col("severity")).contains("HIGH"), F.lit(1))
        .when(F.upper(F.col("severity")).contains("MEDIUM"), F.lit(2))
        .otherwise(F.lit(3)),
    )
)

gold_quality_rule_summary = add_gold_metadata(
    gold_quality_rule_summary,
    f"{SILVER_SCHEMA}.silver_quality_rule_summary",
)


# -----------------------------------------------------------------------------
# Gold RCA context
# -----------------------------------------------------------------------------

scenario_lookup = (
    silver_scenarios
    .select(
        F.col("scenario_id"),
        F.col("scenario_name"),
        F.col("description").alias("scenario_description"),
        F.col("affected_facility_id"),
        F.col("affected_meter_ids"),
        F.col("expected_exception_type"),
        F.col("expected_root_cause"),
        F.col("recommended_next_step"),
    )
)

gold_rca_context = (
    gold_exception_summary_daily.alias("ex")
    .join(
        scenario_lookup.alias("sc"),
        (F.col("ex.facility_id") == F.col("sc.affected_facility_id"))
        & (F.col("ex.exception_type") == F.col("sc.expected_exception_type")),
        "left",
    )
    .select(
        F.col("ex.exception_group_id"),
        F.col("ex.production_date"),
        F.col("ex.facility_id"),
        F.col("ex.facility_name"),
        F.col("ex.region"),
        F.col("ex.basin"),
        F.col("ex.exception_type"),
        F.col("ex.severity"),
        F.col("ex.exception_count"),
        F.col("ex.affected_reading_count"),
        F.col("ex.affected_meter_count"),
        F.array_join(F.col("ex.source_systems"), ", ").alias("source_systems_text"),
        F.array_join(F.col("ex.polling_platforms"), ", ").alias("polling_platforms_text"),
        F.array_join(F.col("ex.device_statuses"), ", ").alias("device_statuses_text"),
        F.array_join(F.col("ex.communication_statuses"), ", ").alias("communication_statuses_text"),
        F.array_join(F.col("ex.signal_qualities"), ", ").alias("signal_qualities_text"),
        F.col("sc.scenario_id"),
        F.col("sc.scenario_name"),
        F.col("sc.scenario_description"),
        F.col("sc.expected_root_cause"),
        F.col("sc.recommended_next_step"),
    )
    .withColumn(
        "rca_summary_seed",
        F.concat_ws(
            " ",
            F.lit("On"),
            F.col("production_date").cast("string"),
            F.lit("facility"),
            F.coalesce(F.col("facility_id"), F.lit("UNKNOWN")),
            F.lit("has"),
            F.col("exception_count").cast("string"),
            F.lit("exception records of type"),
            F.coalesce(F.col("exception_type"), F.lit("UNKNOWN")),
            F.lit("with severity"),
            F.coalesce(F.col("severity"), F.lit("UNKNOWN")),
            F.lit(". Device context:"),
            F.coalesce(F.col("device_statuses_text"), F.lit("none")),
            F.lit(". Communication context:"),
            F.coalesce(F.col("communication_statuses_text"), F.lit("none")),
            F.lit(". Suggested next step:"),
            F.coalesce(
                F.col("recommended_next_step"),
                F.lit("review source data, mapping, timing, and recent pipeline runs."),
            ),
        ),
    )
)

gold_rca_context = add_gold_metadata(
    gold_rca_context,
    "gold_exception_summary_daily + silver_known_issue_scenarios",
)


# -----------------------------------------------------------------------------
# Write Gold tables except reconciliation first
# -----------------------------------------------------------------------------

print("=" * 90)
print("Writing Gold tables...")

write_summary = []
write_counts = {}

tables_to_write = [
    ("dim_facility", dim_facility, 25),
    ("dim_meter", dim_meter, 125),
    ("fact_meter_reading_daily", fact_meter_reading_daily, None),
    ("fact_facility_volume_daily", fact_facility_volume_daily, None),
    ("fact_actual_vs_nominated_daily", fact_actual_vs_nominated_daily, None),
    ("fact_flowcal_reconciliation_daily", fact_flowcal_reconciliation_daily, None),
    ("fact_data_quality_exception", fact_data_quality_exception, 9_055),
    ("fact_pipeline_run", fact_pipeline_run, 150),
    ("fact_support_ticket", fact_support_ticket, 350),
    ("exception_summary_daily", gold_exception_summary_daily, None),
    ("pipeline_health_summary", gold_pipeline_health_summary, None),
    ("quality_rule_summary", gold_quality_rule_summary, 11),
    ("rca_context", gold_rca_context, None),
]

for table_name, df, expected_rows in tables_to_write:
    written_rows = write_gold_table(df, table_name)
    write_counts[table_name] = written_rows

    if expected_rows is None:
        status = "CREATED"
        expected_display = None
    elif written_rows == expected_rows:
        status = "PASS"
        expected_display = expected_rows
    else:
        status = "CHECK"
        expected_display = expected_rows

    write_summary.append(
        {
            "table_name": f"{GOLD_SCHEMA}.{table_name}",
            "expected_rows": expected_display,
            "written_rows": written_rows,
            "status": status,
        }
    )


# -----------------------------------------------------------------------------
# Gold source-to-target reconciliation
# -----------------------------------------------------------------------------

gold_reconciliation_rows = [
    {
        "stage_order": 1,
        "layer": "source",
        "object_name": "raw_polling_readings.csv",
        "row_count": 180_056,
        "metric_type": "raw_source_rows",
        "notes": "Generated structured CSV source records.",
    },
    {
        "stage_order": 2,
        "layer": "bronze",
        "object_name": "bronze_raw_polling_readings",
        "row_count": table(f"{BRONZE_SCHEMA}.bronze_raw_polling_readings").count(),
        "metric_type": "bronze_raw_rows",
        "notes": "Raw CSV records landed in Databricks Bronze.",
    },
    {
        "stage_order": 3,
        "layer": "silver",
        "object_name": "silver_meter_reading_enriched",
        "row_count": silver_readings.count(),
        "metric_type": "silver_enriched_rows",
        "notes": "Standardized readings joined to master and MongoDB context.",
    },
    {
        "stage_order": 4,
        "layer": "silver",
        "object_name": "silver_meter_reading_valid",
        "row_count": table(f"{SILVER_SCHEMA}.silver_meter_reading_valid").count(),
        "metric_type": "silver_valid_or_review_rows",
        "notes": "Records classified as VALID or REVIEW.",
    },
    {
        "stage_order": 5,
        "layer": "silver",
        "object_name": "silver_meter_reading_exceptions",
        "row_count": table(f"{SILVER_SCHEMA}.silver_meter_reading_exceptions").count(),
        "metric_type": "silver_exception_rows",
        "notes": "Records with one or more DQ flags.",
    },
    {
        "stage_order": 6,
        "layer": "gold",
        "object_name": "fact_meter_reading_daily",
        "row_count": write_counts["fact_meter_reading_daily"],
        "metric_type": "gold_meter_day_rows",
        "notes": "Aggregated meter-day measurement fact.",
    },
    {
        "stage_order": 7,
        "layer": "gold",
        "object_name": "fact_facility_volume_daily",
        "row_count": write_counts["fact_facility_volume_daily"],
        "metric_type": "gold_facility_day_rows",
        "notes": "Aggregated facility-day KPI fact.",
    },
    {
        "stage_order": 8,
        "layer": "gold",
        "object_name": "fact_data_quality_exception",
        "row_count": write_counts["fact_data_quality_exception"],
        "metric_type": "gold_exception_fact_rows",
        "notes": "One row per failed DQ rule per affected reading.",
    },
]

fact_source_to_target_reconciliation = spark.createDataFrame(gold_reconciliation_rows)

fact_source_to_target_reconciliation = add_gold_metadata(
    fact_source_to_target_reconciliation,
    "source + bronze + silver + gold row-count checkpoints",
)

reconciliation_count = write_gold_table(
    fact_source_to_target_reconciliation,
    "fact_source_to_target_reconciliation",
)

write_counts["fact_source_to_target_reconciliation"] = reconciliation_count

write_summary.append(
    {
        "table_name": f"{GOLD_SCHEMA}.fact_source_to_target_reconciliation",
        "expected_rows": None,
        "written_rows": reconciliation_count,
        "status": "CREATED",
    }
)


# -----------------------------------------------------------------------------
# Display summary and validation outputs
# -----------------------------------------------------------------------------

summary_df = spark.createDataFrame(write_summary)

print("=" * 90)
print("Gold metrics complete.")
print(f"Gold schema/database: {GOLD_SCHEMA}")
print(f"Gold run ID: {GOLD_RUN_ID}")

display(summary_df.orderBy("table_name"))

failed_df = summary_df.filter(F.col("status") == "CHECK")
failed_count = failed_df.count()

if failed_count > 0:
    print("WARNING: One or more Gold tables had unexpected row counts.")
    display(failed_df)
else:
    print("Gold fixed-count validation checks passed.")


print("=" * 90)
print("Gold source-to-target reconciliation:")
display(
    spark.table(f"{GOLD_SCHEMA}.fact_source_to_target_reconciliation")
    .orderBy("stage_order")
)


print("=" * 90)
print("Gold facility KPI sample:")
display(
    spark.table(f"{GOLD_SCHEMA}.fact_facility_volume_daily")
    .select(
        "reporting_facility_id",
        "master_facility_name",
        "master_region",
        "master_product",
        "production_date",
        "meter_count",
        "hourly_reading_count",
        "accepted_or_review_volume",
        "exception_reading_count",
        "exception_rate",
        "reporting_readiness_score",
    )
    .orderBy("production_date", "reporting_facility_id")
    .limit(25)
)


print("=" * 90)
print("Gold actual vs nominated sample:")
display(
    spark.table(f"{GOLD_SCHEMA}.fact_actual_vs_nominated_daily")
    .select(
        "facility_id",
        "facility_name",
        "production_date",
        "product",
        "actual_volume",
        "nominated_volume",
        "actual_vs_nominated_delta",
        "actual_vs_nominated_pct",
        "reporting_readiness_score",
    )
    .orderBy("production_date", "facility_id")
    .limit(25)
)


print("=" * 90)
print("Gold exception summary:")
display(
    spark.table(f"{GOLD_SCHEMA}.exception_summary_daily")
    .orderBy(F.desc("exception_count"))
    .limit(25)
)


print("=" * 90)
print("Gold pipeline health summary:")
display(
    spark.table(f"{GOLD_SCHEMA}.pipeline_health_summary")
    .orderBy(F.desc("failed_count"), F.desc("partial_load_count"))
)


print("=" * 90)
print("Gold RCA context sample:")
display(
    spark.table(f"{GOLD_SCHEMA}.rca_context")
    .select(
        "production_date",
        "facility_id",
        "facility_name",
        "exception_type",
        "severity",
        "exception_count",
        "device_statuses_text",
        "communication_statuses_text",
        "scenario_name",
        "expected_root_cause",
        "recommended_next_step",
        "rca_summary_seed",
    )
    .orderBy(F.desc("exception_count"))
    .limit(25)
)


print("=" * 90)
print("Gold tables:")
display(spark.sql("SHOW TABLES IN " + GOLD_SCHEMA))