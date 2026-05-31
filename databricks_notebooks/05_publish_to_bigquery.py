"""
MeterFlow IQ - 05 Publish to BigQuery

Purpose:
Publish selected Databricks Gold tables into BigQuery so Streamlit can use
BigQuery as the default investigation / analytics source.

Authentication:
Uses a temporary Google OAuth access token provided through a Databricks
notebook parameter named gcp_access_token.

Do not hard-code tokens or credentials in this file.
"""

import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, MapType, DecimalType


# -----------------------------------------------------------------------------
# Ensure required Python libraries are available
# -----------------------------------------------------------------------------

try:
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2.credentials import Credentials
except ImportError:
    print("Installing Google BigQuery Python libraries...")
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "google-cloud-bigquery[pandas]",
            "google-auth",
            "pandas",
            "pyarrow",
        ]
    )

    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2.credentials import Credentials


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

GOLD_SCHEMA = "meterflow_iq_gold"

PUBLISH_RUN_ID = f"bigquery_publish_{uuid.uuid4().hex[:12]}"

DEFAULT_PROJECT_ID = "project-616f71e8-6bb8-4927-978"
DEFAULT_DATASET_ID = "meterflow_iq_curated"
DEFAULT_LOCATION = "US"

GOLD_TABLES_TO_PUBLISH = [
    {
        "source_table": f"{GOLD_SCHEMA}.dim_facility",
        "target_table": "dim_facility",
        "expected_rows": 25,
        "description": "Facility dimension.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.dim_meter",
        "target_table": "dim_meter",
        "expected_rows": 125,
        "description": "Meter dimension.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_meter_reading_daily",
        "target_table": "fact_meter_reading_daily",
        "expected_rows": 7509,
        "description": "Meter-day measurement fact.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_facility_volume_daily",
        "target_table": "fact_facility_volume_daily",
        "expected_rows": 3663,
        "description": "Facility-day KPI fact.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_actual_vs_nominated_daily",
        "target_table": "fact_actual_vs_nominated_daily",
        "expected_rows": 3663,
        "description": "Actual-vs-nominated KPI fact.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_flowcal_reconciliation_daily",
        "target_table": "fact_flowcal_reconciliation_daily",
        "expected_rows": 7509,
        "description": "FlowCal/source reconciliation fact.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_data_quality_exception",
        "target_table": "fact_data_quality_exception",
        "expected_rows": 9055,
        "description": "One row per failed DQ rule per affected reading.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_pipeline_run",
        "target_table": "fact_pipeline_run",
        "expected_rows": 150,
        "description": "Pipeline run observability fact.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_support_ticket",
        "target_table": "fact_support_ticket",
        "expected_rows": 350,
        "description": "Support ticket / RCA context fact.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.exception_summary_daily",
        "target_table": "exception_summary_daily",
        "expected_rows": 371,
        "description": "Daily exception summary.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.pipeline_health_summary",
        "target_table": "pipeline_health_summary",
        "expected_rows": 5,
        "description": "Pipeline health summary.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.quality_rule_summary",
        "target_table": "quality_rule_summary",
        "expected_rows": 11,
        "description": "DQ rule summary.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.fact_source_to_target_reconciliation",
        "target_table": "fact_source_to_target_reconciliation",
        "expected_rows": 8,
        "description": "Source-to-target reconciliation checkpoints.",
    },
    {
        "source_table": f"{GOLD_SCHEMA}.rca_context",
        "target_table": "rca_context",
        "expected_rows": 371,
        "description": "Facts-only RCA context for Streamlit / AI helper.",
    },
]


print("Spark version:", spark.version)
print("Publish run ID:", PUBLISH_RUN_ID)
print("Gold schema:", GOLD_SCHEMA)


# -----------------------------------------------------------------------------
# Notebook parameters
# -----------------------------------------------------------------------------

def get_or_create_text_widget(name: str, default_value: str, label: str) -> str:
    """
    Get an existing Databricks text widget value, or create the widget if missing.
    """
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        dbutils.widgets.text(name, default_value, label)
        return dbutils.widgets.get(name).strip()


try:
    GCP_ACCESS_TOKEN = get_or_create_text_widget(
        "gcp_access_token",
        "",
        "GCP access token",
    )

    BIGQUERY_PROJECT_ID = get_or_create_text_widget(
        "bigquery_project_id",
        DEFAULT_PROJECT_ID,
        "BigQuery project ID",
    )

    BIGQUERY_DATASET_ID = get_or_create_text_widget(
        "bigquery_dataset_id",
        DEFAULT_DATASET_ID,
        "BigQuery dataset ID",
    )

    BIGQUERY_LOCATION = get_or_create_text_widget(
        "bigquery_location",
        DEFAULT_LOCATION,
        "BigQuery location",
    )

except Exception:
    GCP_ACCESS_TOKEN = os.getenv("GCP_ACCESS_TOKEN", "").strip()
    BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", DEFAULT_PROJECT_ID).strip()
    BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", DEFAULT_DATASET_ID).strip()
    BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", DEFAULT_LOCATION).strip()


if not GCP_ACCESS_TOKEN:
    raise RuntimeError(
        "GCP access token is blank. Generate a fresh token in Google Cloud Shell "
        "using `gcloud auth print-access-token`, paste it into the "
        "`gcp_access_token` notebook parameter, then rerun this notebook."
    )


print("BigQuery project ID:", BIGQUERY_PROJECT_ID)
print("BigQuery dataset ID:", BIGQUERY_DATASET_ID)
print("BigQuery location:", BIGQUERY_LOCATION)


# -----------------------------------------------------------------------------
# Create BigQuery client
# -----------------------------------------------------------------------------

credentials = Credentials(
    token=GCP_ACCESS_TOKEN,
    quota_project_id=BIGQUERY_PROJECT_ID,
)

client = bigquery.Client(
    project=BIGQUERY_PROJECT_ID,
    credentials=credentials,
    location=BIGQUERY_LOCATION,
)

print("BigQuery client created.")


# -----------------------------------------------------------------------------
# Validate dataset and simple query
# -----------------------------------------------------------------------------

dataset_ref = bigquery.DatasetReference(
    BIGQUERY_PROJECT_ID,
    BIGQUERY_DATASET_ID,
)

dataset = client.get_dataset(dataset_ref)

print("Dataset found:")
print(" - full_dataset_id:", dataset.full_dataset_id)
print(" - location:", dataset.location)

query_job = client.query(
    "SELECT 1 AS connection_test",
    location=BIGQUERY_LOCATION,
)

query_result = list(query_job.result())

print("Simple query result:", query_result[0]["connection_test"])


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def normalize_spark_dataframe_for_bigquery(df):
    """
    Convert complex Spark columns into BigQuery-friendly scalar columns.

    BigQuery can support nested/repeated types, but for this portfolio MVP,
    we keep the published investigation tables simple by converting arrays,
    structs, and maps to JSON strings. This makes Streamlit queries easier.
    """
    normalized_df = df

    for field in normalized_df.schema.fields:
        if isinstance(field.dataType, (ArrayType, StructType, MapType)):
            normalized_df = normalized_df.withColumn(
                field.name,
                F.to_json(F.col(field.name)),
            )
        elif isinstance(field.dataType, DecimalType):
            normalized_df = normalized_df.withColumn(
                field.name,
                F.col(field.name).cast("double"),
            )

    return normalized_df


def publish_spark_table_to_bigquery(
    source_table: str,
    target_table: str,
    expected_rows: int | None,
    description: str,
) -> dict:
    """
    Read one Databricks Gold table, convert to pandas, and overwrite
    the matching BigQuery table.
    """
    print("-" * 90)
    print(f"Publishing source table: {source_table}")
    print(f"Target BigQuery table:   {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{target_table}")

    source_df = spark.table(source_table)
    source_rows = source_df.count()

    publish_df = (
        normalize_spark_dataframe_for_bigquery(source_df)
        .withColumn("_bigquery_publish_run_id", F.lit(PUBLISH_RUN_ID))
        .withColumn("_bigquery_published_at_utc", F.current_timestamp())
    )

    pandas_df = publish_df.toPandas()

    target_table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{target_table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_dataframe(
        pandas_df,
        target_table_id,
        job_config=job_config,
        location=BIGQUERY_LOCATION,
    )

    load_job.result()

    target_bq_table = client.get_table(target_table_id)
    written_rows = target_bq_table.num_rows

    if expected_rows is None:
        status = "CREATED"
    elif source_rows == expected_rows and written_rows == expected_rows:
        status = "PASS"
    else:
        status = "CHECK"

    print(f"Description:   {description}")
    print(f"Expected rows: {expected_rows if expected_rows is not None else 'N/A'}")
    print(f"Source rows:   {source_rows:,}")
    print(f"Written rows:  {written_rows:,}")
    print(f"Status:        {status}")

    return {
        "publish_run_id": PUBLISH_RUN_ID,
        "source_table": source_table,
        "target_table": target_table_id,
        "description": description,
        "expected_rows": expected_rows,
        "source_rows": source_rows,
        "written_rows": written_rows,
        "status": status,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def create_or_replace_view(view_name: str, select_sql: str) -> dict:
    """
    Create or replace a BigQuery view in the target dataset.
    """
    view_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{view_name}"

    ddl = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    {select_sql}
    """

    print("-" * 90)
    print(f"Creating / replacing view: {view_id}")

    query_job = client.query(
        ddl,
        location=BIGQUERY_LOCATION,
    )

    query_job.result()

    print(f"View created: {view_id}")

    return {
        "view_name": view_id,
        "status": "CREATED",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }


# -----------------------------------------------------------------------------
# Publish Gold tables
# -----------------------------------------------------------------------------

publish_summary_rows = []

for table_config in GOLD_TABLES_TO_PUBLISH:
    result = publish_spark_table_to_bigquery(
        source_table=table_config["source_table"],
        target_table=table_config["target_table"],
        expected_rows=table_config["expected_rows"],
        description=table_config["description"],
    )

    publish_summary_rows.append(result)


# -----------------------------------------------------------------------------
# Publish BigQuery summary table
# -----------------------------------------------------------------------------

summary_table_id = (
    f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}._databricks_publish_summary"
)

summary_df = pd.DataFrame(publish_summary_rows)

summary_job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

summary_load_job = client.load_table_from_dataframe(
    summary_df,
    summary_table_id,
    job_config=summary_job_config,
    location=BIGQUERY_LOCATION,
)

summary_load_job.result()

summary_table = client.get_table(summary_table_id)

print("=" * 90)
print("Publish summary table written:")
print(" - table:", summary_table_id)
print(" - rows:", summary_table.num_rows)


# -----------------------------------------------------------------------------
# Create Streamlit-friendly BigQuery views
# -----------------------------------------------------------------------------

views_to_create = {
    "vw_streamlit_pipeline_health": f"""
        SELECT
          pipeline_name,
          latest_run_id,
          latest_status,
          latest_run_started_at,
          latest_run_completed_at,
          latest_duration_minutes,
          latest_rows_read,
          latest_rows_accepted,
          latest_rows_rejected,
          latest_error_message,
          run_count,
          success_count,
          failed_count,
          partial_load_count,
          runs_with_errors_count,
          failure_rate,
          overall_rejected_rate,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.pipeline_health_summary`
    """,
    "vw_streamlit_exception_detail": f"""
        SELECT
          exception_id,
          rule_id,
          exception_type,
          severity,
          business_reason,
          raw_reading_id,
          meter_id,
          facility_id,
          facility_name,
          region,
          basin,
          production_date,
          reading_timestamp,
          source_system,
          polling_platform,
          volume,
          quality_code,
          raw_status,
          mongo_device_status,
          mongo_communication_status,
          mongo_signal_quality,
          mongo_scenario_id,
          primary_exception_type,
          exception_status,
          assigned_to,
          analyst_notes,
          days_since_production_date,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_data_quality_exception`
    """,
    "vw_streamlit_exception_summary": f"""
        SELECT
          production_date,
          facility_id,
          facility_name,
          region,
          basin,
          exception_type,
          severity,
          exception_rule_failure_count,
          exception_count,
          affected_reading_count,
          affected_meter_count,
          source_systems,
          polling_platforms,
          device_statuses,
          communication_statuses,
          signal_qualities,
          first_detected_at,
          last_detected_at,
          exception_group_id,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.exception_summary_daily`
    """,
    "vw_streamlit_facility_kpi_daily": f"""
        SELECT
          reporting_facility_id,
          master_facility_name,
          master_region,
          master_basin,
          master_state,
          master_asset_type,
          master_operator,
          master_product,
          production_date,
          gas_day,
          meter_count,
          hourly_reading_count,
          valid_reading_count,
          review_reading_count,
          exception_reading_count,
          valid_volume,
          accepted_or_review_volume,
          all_nonnegative_volume,
          raw_volume_sum,
          exception_rate,
          valid_record_rate,
          reporting_readiness_score,
          has_data_quality_issue,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_facility_volume_daily`
    """,
    "vw_streamlit_actual_vs_nominated": f"""
        SELECT
          facility_id,
          facility_name,
          region,
          basin,
          state,
          production_date,
          product,
          actual_volume,
          valid_volume,
          nominated_volume,
          actual_vs_nominated_delta,
          actual_vs_nominated_pct,
          nomination_count,
          exception_rate,
          reporting_readiness_score,
          has_data_quality_issue,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_actual_vs_nominated_daily`
    """,
    "vw_streamlit_reconciliation": f"""
        SELECT
          stage_order,
          layer,
          object_name,
          row_count,
          metric_type,
          notes,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.fact_source_to_target_reconciliation`
    """,
    "vw_streamlit_rca_context": f"""
        SELECT
          exception_group_id,
          production_date,
          facility_id,
          facility_name,
          region,
          basin,
          exception_type,
          severity,
          exception_count,
          affected_reading_count,
          affected_meter_count,
          source_systems_text,
          polling_platforms_text,
          device_statuses_text,
          communication_statuses_text,
          signal_qualities_text,
          scenario_id,
          scenario_name,
          scenario_description,
          expected_root_cause,
          recommended_next_step,
          rca_summary_seed,
          _bigquery_publish_run_id,
          _bigquery_published_at_utc
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.rca_context`
    """,
}

view_summary_rows = []

for view_name, select_sql in views_to_create.items():
    view_result = create_or_replace_view(
        view_name=view_name,
        select_sql=select_sql,
    )

    view_summary_rows.append(view_result)


# -----------------------------------------------------------------------------
# Validate published tables
# -----------------------------------------------------------------------------

summary_query = f"""
SELECT
  target_table,
  expected_rows,
  source_rows,
  written_rows,
  status,
  published_at_utc
FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}._databricks_publish_summary`
ORDER BY target_table
"""

summary_job = client.query(
    summary_query,
    location=BIGQUERY_LOCATION,
)

summary_rows = list(summary_job.result())

print("=" * 90)
print("BigQuery publish summary:")

for row in summary_rows:
    print(dict(row))


failed_rows = [
    dict(row)
    for row in summary_rows
    if row["status"] == "CHECK"
]

if failed_rows:
    print("=" * 90)
    print("WARNING: One or more published BigQuery tables had row-count issues.")
    for row in failed_rows:
        print(row)
else:
    print("=" * 90)
    print("All fixed-count BigQuery publish checks passed.")


# -----------------------------------------------------------------------------
# List current BigQuery tables/views in target dataset
# -----------------------------------------------------------------------------

print("=" * 90)
print("Current BigQuery tables/views in target dataset:")

tables = list(client.list_tables(dataset_ref))

for table_item in sorted(tables, key=lambda item: item.table_id):
    print(f" - {table_item.table_id} ({table_item.table_type})")


print("=" * 90)
print("BigQuery Gold publish complete.")
print("Status: PASS")
print("Publish run ID:", PUBLISH_RUN_ID)