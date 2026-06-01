"""
MeterFlow IQ - 06 Publish to Snowflake Business

Publishes selected Databricks Gold tables into Snowflake for the
Power BI Business KPI Dashboard.

Current target:
    Snowflake database: METERFLOW_IQ
    Snowflake schema:   CURATED

This publisher is intentionally business-facing. It publishes:
    - facility and meter dimensions
    - meter/facility daily facts
    - actual-vs-nominated facts
    - FlowCal reconciliation facts
    - daily exception summary
    - DQ rule summary
    - pipeline readiness summary

Authentication:
    Use Databricks notebook parameters/widgets or environment variables.

Do not commit Snowflake passwords or secrets.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd


# -----------------------------------------------------------------------------
# Runtime package install
# -----------------------------------------------------------------------------
# This uses the Snowflake Python connector because this portfolio data volume is
# intentionally small. For large production loads, prefer the Spark/Snowflake
# connector or staged COPY patterns.
# -----------------------------------------------------------------------------

def install_snowflake_connector_if_needed() -> None:
    """
    Install Snowflake connector if it is not already available.
    """
    try:
        import snowflake.connector  # noqa: F401
        from snowflake.connector.pandas_tools import write_pandas  # noqa: F401
        print("Snowflake connector import: OK")
    except Exception:
        print("Installing snowflake-connector-python[pandas]...")
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "snowflake-connector-python[pandas]",
            ]
        )

        import snowflake.connector  # noqa: F401
        from snowflake.connector.pandas_tools import write_pandas  # noqa: F401

        print("Snowflake connector installed/imported: OK")


install_snowflake_connector_if_needed()

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# -----------------------------------------------------------------------------
# Databricks widget helpers
# -----------------------------------------------------------------------------

try:
    _dbutils = dbutils  # type: ignore[name-defined]
except NameError:
    _dbutils = None


def ensure_widget(name: str, default_value: str, label: str) -> None:
    """
    Create a Databricks widget if running inside Databricks and it does not exist.
    """
    if _dbutils is None:
        return

    try:
        _dbutils.widgets.get(name)
    except Exception:
        _dbutils.widgets.text(name, default_value, label)


def get_config_value(
    widget_name: str,
    env_name: str,
    default_value: str = "",
    label: Optional[str] = None,
) -> str:
    """
    Read config from Databricks widget first, then environment variable,
    then default value.
    """
    if label is None:
        label = widget_name

    ensure_widget(widget_name, default_value, label)

    widget_value = ""

    if _dbutils is not None:
        try:
            widget_value = _dbutils.widgets.get(widget_name).strip()
        except Exception:
            widget_value = ""

    env_value = os.getenv(env_name, "").strip()

    if widget_value:
        return widget_value

    if env_value:
        return env_value

    return default_value.strip()


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

GOLD_SCHEMA = get_config_value(
    widget_name="gold_schema",
    env_name="METERFLOW_GOLD_SCHEMA",
    default_value="meterflow_iq_gold",
    label="Databricks Gold schema",
)

SNOWFLAKE_ACCOUNT_RAW = get_config_value(
    widget_name="sf_account",
    env_name="SNOWFLAKE_ACCOUNT",
    default_value="",
    label="Snowflake account identifier",
)

SNOWFLAKE_USER = get_config_value(
    widget_name="sf_user",
    env_name="SNOWFLAKE_USER",
    default_value="",
    label="Snowflake username",
)

SNOWFLAKE_PASSWORD = get_config_value(
    widget_name="sf_password",
    env_name="SNOWFLAKE_PASSWORD",
    default_value="",
    label="Snowflake password",
)

SNOWFLAKE_ROLE = get_config_value(
    widget_name="sf_role",
    env_name="SNOWFLAKE_ROLE",
    default_value="METERFLOW_IQ_ROLE",
    label="Snowflake role",
).upper()

SNOWFLAKE_WAREHOUSE = get_config_value(
    widget_name="sf_warehouse",
    env_name="SNOWFLAKE_WAREHOUSE",
    default_value="METERFLOW_IQ_WH",
    label="Snowflake warehouse",
).upper()

SNOWFLAKE_DATABASE = get_config_value(
    widget_name="sf_database",
    env_name="SNOWFLAKE_DATABASE",
    default_value="METERFLOW_IQ",
    label="Snowflake database",
).upper()

SNOWFLAKE_SCHEMA = get_config_value(
    widget_name="sf_schema",
    env_name="SNOWFLAKE_SCHEMA",
    default_value="CURATED",
    label="Snowflake schema",
).upper()


def normalize_snowflake_account(account_value: str) -> str:
    """
    Normalize Snowflake account value.

    Accepts either:
      - HPFGDZY-JM52187
      - HPFGDZY-JM52187.snowflakecomputing.com
      - https://HPFGDZY-JM52187.snowflakecomputing.com
    """
    value = account_value.strip()

    value = value.replace("https://", "").replace("http://", "")
    value = value.split("/")[0]

    if value.lower().endswith(".snowflakecomputing.com"):
        value = value[: -len(".snowflakecomputing.com")]

    return value


SNOWFLAKE_ACCOUNT = normalize_snowflake_account(SNOWFLAKE_ACCOUNT_RAW)


def validate_required_config() -> None:
    """
    Validate Snowflake config before attempting to connect.
    """
    missing_values: List[str] = []

    if not SNOWFLAKE_ACCOUNT:
        missing_values.append("sf_account / SNOWFLAKE_ACCOUNT")

    if not SNOWFLAKE_USER:
        missing_values.append("sf_user / SNOWFLAKE_USER")

    if not SNOWFLAKE_PASSWORD:
        missing_values.append("sf_password / SNOWFLAKE_PASSWORD")

    if not SNOWFLAKE_ROLE:
        missing_values.append("sf_role / SNOWFLAKE_ROLE")

    if not SNOWFLAKE_WAREHOUSE:
        missing_values.append("sf_warehouse / SNOWFLAKE_WAREHOUSE")

    if not SNOWFLAKE_DATABASE:
        missing_values.append("sf_database / SNOWFLAKE_DATABASE")

    if not SNOWFLAKE_SCHEMA:
        missing_values.append("sf_schema / SNOWFLAKE_SCHEMA")

    if missing_values:
        raise RuntimeError(
            "Missing required Snowflake configuration values: "
            + ", ".join(missing_values)
            + ". Enter them as Databricks notebook parameters/widgets. "
            + "Do not put passwords in Git."
        )


validate_required_config()


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

PUBLISH_RUN_ID = f"snowflake_business_{uuid.uuid4().hex[:12]}"
PUBLISHED_AT_UTC = datetime.now(timezone.utc).isoformat()

BUSINESS_TABLE_SPECS: List[Dict[str, Any]] = [
    {
        "source_table": "dim_facility",
        "target_table": "DIM_FACILITY",
        "expected_rows": 25,
        "description": "Facility dimension for business KPI reporting.",
    },
    {
        "source_table": "dim_meter",
        "target_table": "DIM_METER",
        "expected_rows": 125,
        "description": "Meter dimension for business KPI reporting.",
    },
    {
        "source_table": "fact_meter_reading_daily",
        "target_table": "FACT_METER_READING_DAILY",
        "expected_rows": 7509,
        "description": "Meter-day measurement fact.",
    },
    {
        "source_table": "fact_facility_volume_daily",
        "target_table": "FACT_FACILITY_VOLUME_DAILY",
        "expected_rows": 3663,
        "description": "Facility-day volume KPI fact.",
    },
    {
        "source_table": "fact_actual_vs_nominated_daily",
        "target_table": "FACT_ACTUAL_VS_NOMINATED_DAILY",
        "expected_rows": 3663,
        "description": "Actual-vs-nominated facility-day KPI fact.",
    },
    {
        "source_table": "fact_flowcal_reconciliation_daily",
        "target_table": "FACT_FLOWCAL_RECONCILIATION_DAILY",
        "expected_rows": 7509,
        "description": "FlowCal/source reconciliation fact.",
    },
    {
        "source_table": "exception_summary_daily",
        "target_table": "EXCEPTION_SUMMARY_DAILY",
        "expected_rows": 371,
        "description": "Daily exception summary for business impact reporting.",
    },
    {
        "source_table": "quality_rule_summary",
        "target_table": "QUALITY_RULE_SUMMARY",
        "expected_rows": 11,
        "description": "Data-quality rule summary.",
    },
    {
        "source_table": "pipeline_health_summary",
        "target_table": "PIPELINE_HEALTH_SUMMARY",
        "expected_rows": 5,
        "description": "Pipeline/reporting readiness summary.",
    },
]

BUSINESS_VIEW_SPECS: List[Dict[str, str]] = [
    {
        "view_name": "VW_BUSINESS_KPI_DAILY",
        "source_table": "FACT_FACILITY_VOLUME_DAILY",
        "description": "Business KPI daily facility volume view.",
    },
    {
        "view_name": "VW_BUSINESS_FACILITY_VOLUME_TREND",
        "source_table": "FACT_FACILITY_VOLUME_DAILY",
        "description": "Facility volume trend view.",
    },
    {
        "view_name": "VW_BUSINESS_ACTUAL_VS_NOMINATED",
        "source_table": "FACT_ACTUAL_VS_NOMINATED_DAILY",
        "description": "Actual-vs-nominated business variance view.",
    },
    {
        "view_name": "VW_BUSINESS_FLOWCAL_RECONCILIATION",
        "source_table": "FACT_FLOWCAL_RECONCILIATION_DAILY",
        "description": "FlowCal/source reconciliation view.",
    },
    {
        "view_name": "VW_BUSINESS_DATA_QUALITY_IMPACT",
        "source_table": "EXCEPTION_SUMMARY_DAILY",
        "description": "Business data-quality impact view.",
    },
    {
        "view_name": "VW_BUSINESS_REPORTING_READINESS",
        "source_table": "PIPELINE_HEALTH_SUMMARY",
        "description": "Reporting readiness and pipeline health view.",
    },
]


# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------

def is_safe_snowflake_identifier(identifier: str) -> bool:
    """
    Return whether identifier contains only safe Snowflake unquoted identifier chars.
    """
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier))


def require_safe_identifier(identifier: str, label: str) -> None:
    """
    Raise if an identifier is unsafe.
    """
    if not is_safe_snowflake_identifier(identifier):
        raise ValueError(f"Unsafe Snowflake {label}: {identifier}")


def uppercase_and_dedupe_columns(columns: List[str]) -> List[str]:
    """
    Convert column names to Snowflake-friendly uppercase names and dedupe if needed.
    """
    seen: Dict[str, int] = {}
    cleaned_columns: List[str] = []

    for raw_column in columns:
        cleaned = re.sub(r"[^A-Za-z0-9_]", "_", str(raw_column).strip())
        cleaned = re.sub(r"_+", "_", cleaned).strip("_").upper()

        if not cleaned:
            cleaned = "COL"

        if cleaned[0].isdigit():
            cleaned = f"COL_{cleaned}"

        count = seen.get(cleaned, 0)

        if count:
            deduped = f"{cleaned}_{count + 1}"
        else:
            deduped = cleaned

        seen[cleaned] = count + 1
        cleaned_columns.append(deduped)

    return cleaned_columns


def normalize_object_value(value: Any) -> Any:
    """
    Normalize object values so pandas/pyarrow can safely write to Snowflake.
    """
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, default=str)

    return value


def spark_dataframe_to_pandas_for_snowflake(spark_df: Any) -> pd.DataFrame:
    """
    Convert a small Spark DataFrame to pandas without using Spark's toPandas().

    In Databricks Serverless / Spark Connect, toPandas() can sometimes carry
    non-data execution metadata into pandas/pyarrow. That can cause errors like:

        TypeError: Object of type PlanMetrics is not JSON serializable

    These Snowflake business publish tables are small enough for manual collect.
    """
    rows = [
        row.asDict(recursive=True)
        for row in spark_df.collect()
    ]

    pandas_df = pd.DataFrame(
        rows,
        columns=spark_df.columns,
    )

    pandas_df.attrs.clear()
    pandas_df = pandas_df.reset_index(drop=True)

    return pandas_df


def normalize_pandas_for_snowflake(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Clean pandas DataFrame for write_pandas.
    """
    output_pdf = pdf.copy(deep=True)

    # Clear pandas metadata so pyarrow/write_pandas does not try to serialize
    # non-data Spark execution metadata.
    output_pdf.attrs.clear()
    output_pdf = output_pdf.reset_index(drop=True)

    output_pdf.columns = uppercase_and_dedupe_columns(list(output_pdf.columns))

    for column_name in output_pdf.columns:
        if output_pdf[column_name].dtype == "object":
            output_pdf[column_name] = output_pdf[column_name].map(
                normalize_object_value
            )

    output_pdf = output_pdf.where(pd.notnull(output_pdf), None)

    output_pdf.attrs.clear()
    output_pdf = output_pdf.reset_index(drop=True)

    return output_pdf


def get_snowflake_connection():
    """
    Create a Snowflake connector connection.
    """
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        client_session_keep_alive=False,
    )


def execute_snowflake_sql(conn, sql: str) -> None:
    """
    Execute a Snowflake SQL statement.
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)


def fetch_one_value(conn, sql: str) -> Any:
    """
    Execute SQL and return the first column from the first row.
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()

    if row is None:
        return None

    return row[0]


def count_snowflake_table(conn, table_name: str) -> int:
    """
    Count rows in a Snowflake table/view.
    """
    require_safe_identifier(table_name, "table/view name")

    result = fetch_one_value(
        conn,
        f'SELECT COUNT(*) FROM "{SNOWFLAKE_DATABASE}"."{SNOWFLAKE_SCHEMA}"."{table_name}"',
    )

    if result is None:
        return 0

    return int(result)


def spark_table_exists(table_name: str) -> bool:
    """
    Return whether a Spark table exists.
    """
    try:
        spark.table(table_name).limit(1).count()  # type: ignore[name-defined]
        return True
    except Exception:
        return False


def safe_display_dataframe(pdf: pd.DataFrame) -> None:
    """
    Display a pandas dataframe in Databricks if possible.
    """
    try:
        display(spark.createDataFrame(pdf))  # type: ignore[name-defined]
    except Exception:
        print(pdf.to_string(index=False))


def write_dataframe_to_snowflake(
    conn,
    pdf: pd.DataFrame,
    target_table: str,
) -> int:
    """
    Write pandas DataFrame to Snowflake and return written row count.
    """
    require_safe_identifier(target_table, "target table")

    normalized_pdf = normalize_pandas_for_snowflake(pdf)

    execute_snowflake_sql(
        conn,
        f'DROP TABLE IF EXISTS "{SNOWFLAKE_DATABASE}"."{SNOWFLAKE_SCHEMA}"."{target_table}"',
    )

    success, chunk_count, row_count, output = write_pandas(
        conn=conn,
        df=normalized_pdf,
        table_name=target_table,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        auto_create_table=True,
        overwrite=True,
        quote_identifiers=False,
    )

    if not success:
        raise RuntimeError(
            f"write_pandas failed for {target_table}. Output: {output}"
        )

    written_rows = count_snowflake_table(conn, target_table)

    print(
        f"Snowflake write complete: {target_table} | "
        f"chunks={chunk_count:,} | write_pandas_rows={row_count:,} | "
        f"snowflake_count={written_rows:,}"
    )

    return written_rows


def create_business_views(conn) -> List[Dict[str, Any]]:
    """
    Create business-facing Snowflake views.
    """
    view_results: List[Dict[str, Any]] = []

    for view_spec in BUSINESS_VIEW_SPECS:
        view_name = view_spec["view_name"]
        source_table = view_spec["source_table"]
        description = view_spec["description"]

        require_safe_identifier(view_name, "view name")
        require_safe_identifier(source_table, "source table")

        print("-" * 90)
        print(
            "Creating / replacing Snowflake view: "
            f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{view_name}"
        )
        print(f"Source table: {source_table}")
        print(f"Description:  {description}")

        view_sql = f"""
CREATE OR REPLACE VIEW "{SNOWFLAKE_DATABASE}"."{SNOWFLAKE_SCHEMA}"."{view_name}" AS
SELECT
  *
FROM "{SNOWFLAKE_DATABASE}"."{SNOWFLAKE_SCHEMA}"."{source_table}";
"""

        execute_snowflake_sql(conn, view_sql)

        view_rows = count_snowflake_table(conn, view_name)

        print(f"View created: {view_name} | rows={view_rows:,}")

        view_results.append(
            {
                "publish_run_id": PUBLISH_RUN_ID,
                "view_name": view_name,
                "source_table": source_table,
                "description": description,
                "view_rows": view_rows,
                "status": "PASS",
                "published_at_utc": PUBLISHED_AT_UTC,
            }
        )

    return view_results


# -----------------------------------------------------------------------------
# Start publish
# -----------------------------------------------------------------------------

print("=" * 90)
print("MeterFlow IQ - Publish Databricks Gold to Snowflake Business Layer")
print("=" * 90)
print(f"Spark version: {spark.version}")  # type: ignore[name-defined]
print(f"Publish run ID: {PUBLISH_RUN_ID}")
print(f"Published at UTC: {PUBLISHED_AT_UTC}")
print(f"Databricks Gold schema: {GOLD_SCHEMA}")
print(f"Snowflake account: {SNOWFLAKE_ACCOUNT}")
print(f"Snowflake user: {SNOWFLAKE_USER}")
print(f"Snowflake role: {SNOWFLAKE_ROLE}")
print(f"Snowflake warehouse: {SNOWFLAKE_WAREHOUSE}")
print(f"Snowflake database: {SNOWFLAKE_DATABASE}")
print(f"Snowflake schema: {SNOWFLAKE_SCHEMA}")
print("Snowflake password: [not printed]")


# -----------------------------------------------------------------------------
# Validate source Gold tables
# -----------------------------------------------------------------------------

print("=" * 90)
print("Validating Databricks Gold source tables...")

for table_spec in BUSINESS_TABLE_SPECS:
    source_fqn = f"{GOLD_SCHEMA}.{table_spec['source_table']}"

    if not spark_table_exists(source_fqn):
        raise RuntimeError(f"Required Gold source table not found: {source_fqn}")

    print(f"Found source table: {source_fqn}")

print("All required Gold source tables found.")


# -----------------------------------------------------------------------------
# Connect to Snowflake and publish
# -----------------------------------------------------------------------------

summary_rows: List[Dict[str, Any]] = []
view_summary_rows: List[Dict[str, Any]] = []

conn = None

try:
    conn = get_snowflake_connection()

    print("=" * 90)
    print("Snowflake connection created.")

    # Set explicit context.
    execute_snowflake_sql(conn, f'USE ROLE "{SNOWFLAKE_ROLE}"')
    execute_snowflake_sql(conn, f'USE WAREHOUSE "{SNOWFLAKE_WAREHOUSE}"')
    execute_snowflake_sql(conn, f'USE DATABASE "{SNOWFLAKE_DATABASE}"')
    execute_snowflake_sql(conn, f'USE SCHEMA "{SNOWFLAKE_SCHEMA}"')

    current_role = fetch_one_value(conn, "SELECT CURRENT_ROLE()")
    current_warehouse = fetch_one_value(conn, "SELECT CURRENT_WAREHOUSE()")
    current_database = fetch_one_value(conn, "SELECT CURRENT_DATABASE()")
    current_schema = fetch_one_value(conn, "SELECT CURRENT_SCHEMA()")

    print("Snowflake context:")
    print(f" - current_role: {current_role}")
    print(f" - current_warehouse: {current_warehouse}")
    print(f" - current_database: {current_database}")
    print(f" - current_schema: {current_schema}")

    # Publish business tables.
    print("=" * 90)
    print("Publishing business tables to Snowflake...")

    for table_spec in BUSINESS_TABLE_SPECS:
        source_table_name = table_spec["source_table"]
        target_table_name = table_spec["target_table"]
        expected_rows = int(table_spec["expected_rows"])
        description = table_spec["description"]
        source_fqn = f"{GOLD_SCHEMA}.{source_table_name}"

        print("-" * 90)
        print(f"Publishing source table: {source_fqn}")
        print(
            "Target Snowflake table: "
            f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{target_table_name}"
        )
        print(f"Description: {description}")
        print(f"Expected rows: {expected_rows:,}")

        spark_df = spark.table(source_fqn)  # type: ignore[name-defined]
        source_rows = spark_df.count()

        print(f"Source rows: {source_rows:,}")

        pandas_df = spark_dataframe_to_pandas_for_snowflake(spark_df)

        written_rows = write_dataframe_to_snowflake(
            conn=conn,
            pdf=pandas_df,
            target_table=target_table_name,
        )

        if source_rows == expected_rows and written_rows == expected_rows:
            status = "PASS"
        elif source_rows == written_rows:
            status = "WARN_EXPECTED_COUNT_CHANGED"
        else:
            status = "FAIL"

        print(f"Written rows: {written_rows:,}")
        print(f"Status: {status}")

        summary_rows.append(
            {
                "publish_run_id": PUBLISH_RUN_ID,
                "source_table": source_fqn,
                "target_table": (
                    f"{SNOWFLAKE_DATABASE}."
                    f"{SNOWFLAKE_SCHEMA}."
                    f"{target_table_name}"
                ),
                "description": description,
                "expected_rows": expected_rows,
                "source_rows": int(source_rows),
                "written_rows": int(written_rows),
                "status": status,
                "published_at_utc": PUBLISHED_AT_UTC,
            }
        )

    # Create summary table.
    print("=" * 90)
    print("Writing Snowflake publish summary table...")

    summary_pdf = pd.DataFrame(summary_rows)

    write_dataframe_to_snowflake(
        conn=conn,
        pdf=summary_pdf,
        target_table="_DATABRICKS_SNOWFLAKE_PUBLISH_SUMMARY",
    )

    print("Publish summary table written:")
    print(
        " - table: "
        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."
        "_DATABRICKS_SNOWFLAKE_PUBLISH_SUMMARY"
    )
    print(f" - rows: {len(summary_rows):,}")

    # Create business views.
    print("=" * 90)
    print("Creating business views...")

    view_summary_rows = create_business_views(conn)

    view_summary_pdf = pd.DataFrame(view_summary_rows)

    write_dataframe_to_snowflake(
        conn=conn,
        pdf=view_summary_pdf,
        target_table="_DATABRICKS_SNOWFLAKE_VIEW_SUMMARY",
    )

    print("View summary table written:")
    print(
        " - table: "
        f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."
        "_DATABRICKS_SNOWFLAKE_VIEW_SUMMARY"
    )
    print(f" - rows: {len(view_summary_rows):,}")

    # Final validation.
    print("=" * 90)
    print("Snowflake publish summary:")
    for row in summary_rows:
        print(row)

    non_pass_rows = [
        row
        for row in summary_rows
        if row["status"] != "PASS"
    ]

    if non_pass_rows:
        print("=" * 90)
        print("WARNING: Some Snowflake publish checks did not return PASS:")
        for row in non_pass_rows:
            print(row)

        raise RuntimeError(
            "One or more Snowflake publish checks failed or changed expected counts."
        )

    print("=" * 90)
    print("All fixed-count Snowflake business publish checks passed.")

    print("=" * 90)
    print("Created / updated Snowflake business tables:")
    for row in summary_rows:
        print(f" - {row['target_table']} ({row['written_rows']:,} rows)")

    print("=" * 90)
    print("Created / updated Snowflake business views:")
    for row in view_summary_rows:
        print(
            f" - {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}."
            f"{row['view_name']} ({row['view_rows']:,} rows)"
        )

    print("=" * 90)
    print("Snowflake Business publish complete.")
    print("Status: PASS")
    print(f"Publish run ID: {PUBLISH_RUN_ID}")

finally:
    if conn is not None:
        try:
            conn.close()
            print("Snowflake connection closed.")
        except Exception:
            pass


# -----------------------------------------------------------------------------
# Display summaries in Databricks output
# -----------------------------------------------------------------------------

if summary_rows:
    summary_display_pdf = pd.DataFrame(summary_rows)
    safe_display_dataframe(summary_display_pdf)

if view_summary_rows:
    view_summary_display_pdf = pd.DataFrame(view_summary_rows)
    safe_display_dataframe(view_summary_display_pdf)