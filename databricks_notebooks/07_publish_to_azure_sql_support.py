"""
MeterFlow IQ - 07 Publish to Azure SQL Support

Publishes selected Databricks Gold technical/support tables into Azure SQL
for the Power BI Data Reliability & Support Ops dashboard.

Current target:
    Azure SQL server:   meterflow-iq-sql-ngp01.database.windows.net
    Azure SQL database: meterflow_iq_support
    Azure SQL schema:   dbo

This publisher is intentionally technical/support-facing. It publishes:
    - pipeline run facts
    - pipeline health summary
    - data-quality exception backlog
    - daily exception summary
    - source-to-target reconciliation
    - support tickets / RCA context
    - DQ rule summary

Authentication:
    Use Databricks notebook parameters/widgets or environment variables.

Do not commit Azure SQL passwords or secrets.
"""

import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


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

AZURE_SQL_SERVER_RAW = get_config_value(
    widget_name="azure_sql_server",
    env_name="AZURE_SQL_SERVER",
    default_value="meterflow-iq-sql-ngp01.database.windows.net",
    label="Azure SQL server",
)

AZURE_SQL_DATABASE = get_config_value(
    widget_name="azure_sql_database",
    env_name="AZURE_SQL_DATABASE",
    default_value="meterflow_iq_support",
    label="Azure SQL database",
)

AZURE_SQL_SCHEMA = get_config_value(
    widget_name="azure_sql_schema",
    env_name="AZURE_SQL_SCHEMA",
    default_value="dbo",
    label="Azure SQL schema",
)

AZURE_SQL_USER = get_config_value(
    widget_name="azure_sql_user",
    env_name="AZURE_SQL_USER",
    default_value="meterflowuser",
    label="Azure SQL username",
)

AZURE_SQL_PASSWORD = get_config_value(
    widget_name="azure_sql_password",
    env_name="AZURE_SQL_PASSWORD",
    default_value="",
    label="Azure SQL password",
)

AZURE_SQL_PORT = get_config_value(
    widget_name="azure_sql_port",
    env_name="AZURE_SQL_PORT",
    default_value="1433",
    label="Azure SQL port",
)


def normalize_azure_sql_server(server_value: str) -> str:
    """
    Normalize Azure SQL server value.

    Accepts:
      - meterflow-iq-sql-ngp01
      - meterflow-iq-sql-ngp01.database.windows.net
      - tcp:meterflow-iq-sql-ngp01.database.windows.net,1433
    """
    value = server_value.strip()
    value = value.replace("tcp:", "")
    value = value.replace("https://", "").replace("http://", "")
    value = value.split("/")[0]
    value = value.split(",")[0]

    if not value.lower().endswith(".database.windows.net"):
        value = f"{value}.database.windows.net"

    return value


AZURE_SQL_SERVER = normalize_azure_sql_server(AZURE_SQL_SERVER_RAW)


def validate_required_config() -> None:
    """
    Validate Azure SQL config before attempting to connect.
    """
    missing_values: List[str] = []

    if not GOLD_SCHEMA:
        missing_values.append("gold_schema / METERFLOW_GOLD_SCHEMA")

    if not AZURE_SQL_SERVER:
        missing_values.append("azure_sql_server / AZURE_SQL_SERVER")

    if not AZURE_SQL_DATABASE:
        missing_values.append("azure_sql_database / AZURE_SQL_DATABASE")

    if not AZURE_SQL_SCHEMA:
        missing_values.append("azure_sql_schema / AZURE_SQL_SCHEMA")

    if not AZURE_SQL_USER:
        missing_values.append("azure_sql_user / AZURE_SQL_USER")

    if not AZURE_SQL_PASSWORD:
        missing_values.append("azure_sql_password / AZURE_SQL_PASSWORD")

    if not AZURE_SQL_PORT:
        missing_values.append("azure_sql_port / AZURE_SQL_PORT")

    if missing_values:
        raise RuntimeError(
            "Missing required Azure SQL configuration values: "
            + ", ".join(missing_values)
            + ". Enter them as Databricks notebook parameters/widgets. "
            + "Do not put passwords in Git."
        )


validate_required_config()


# -----------------------------------------------------------------------------
# SQL Server data source configuration
# -----------------------------------------------------------------------------
# Databricks Serverless rejected generic JDBC writes in this environment.
# Use the Databricks SQL Server data source name: "sqlserver".
#
# Serverless supports a restricted option list. Do not use:
#   - hostNameInCertificate
#   - loginTimeout
#
# Supported options include:
#   authentication, batchsize, connectiontimeout, database, dbtable, debug,
#   encrypt, host, isolationlevel, numpartitions, password, port, querytimeout,
#   truncate, trustservercertificate, user.
# -----------------------------------------------------------------------------

SQLSERVER_FORMAT = "sqlserver"


def base_sqlserver_reader():
    """
    Return a configured Spark reader for Azure SQL / SQL Server.
    """
    return (
        spark.read.format(SQLSERVER_FORMAT)  # type: ignore[name-defined]
        .option("host", AZURE_SQL_SERVER)
        .option("port", AZURE_SQL_PORT)
        .option("database", AZURE_SQL_DATABASE)
        .option("user", AZURE_SQL_USER)
        .option("password", AZURE_SQL_PASSWORD)
        .option("encrypt", "true")
        .option("trustservercertificate", "false")
        .option("connectiontimeout", "60")
        .option("querytimeout", "300")
    )


def base_sqlserver_writer(df: DataFrame):
    """
    Return a configured Spark writer for Azure SQL / SQL Server.
    """
    return (
        df.write.format(SQLSERVER_FORMAT)
        .option("host", AZURE_SQL_SERVER)
        .option("port", AZURE_SQL_PORT)
        .option("database", AZURE_SQL_DATABASE)
        .option("user", AZURE_SQL_USER)
        .option("password", AZURE_SQL_PASSWORD)
        .option("encrypt", "true")
        .option("trustservercertificate", "false")
        .option("connectiontimeout", "60")
        .option("querytimeout", "300")
        .option("batchsize", "1000")
        .option("isolationlevel", "READ_COMMITTED")
    )


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

PUBLISH_RUN_ID = f"azure_sql_support_{uuid.uuid4().hex[:12]}"
PUBLISHED_AT_UTC = datetime.now(timezone.utc)

# SQL Server datetime2-friendly timestamp string.
PUBLISHED_AT_UTC_SQL = (
    PUBLISHED_AT_UTC
    .replace(tzinfo=None)
    .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
)

SUPPORT_TABLE_SPECS: List[Dict[str, Any]] = [
    {
        "source_table": "fact_pipeline_run",
        "target_table": "fact_pipeline_run",
        "expected_rows": 150,
        "description": "Pipeline run observability fact.",
    },
    {
        "source_table": "pipeline_health_summary",
        "target_table": "pipeline_health_summary",
        "expected_rows": 5,
        "description": "Pipeline health and reporting readiness summary.",
    },
    {
        "source_table": "fact_data_quality_exception",
        "target_table": "fact_data_quality_exception",
        "expected_rows": 9055,
        "description": "Data-quality exception backlog fact.",
    },
    {
        "source_table": "exception_summary_daily",
        "target_table": "exception_summary_daily",
        "expected_rows": 371,
        "description": "Daily exception summary for support operations.",
    },
    {
        "source_table": "fact_source_to_target_reconciliation",
        "target_table": "fact_source_to_target_reconciliation",
        "expected_rows": 8,
        "description": "Source-to-target reconciliation checkpoint fact.",
    },
    {
        "source_table": "fact_support_ticket",
        "target_table": "fact_support_ticket",
        "expected_rows": 350,
        "description": "Support ticket and RCA context fact.",
    },
    {
        "source_table": "quality_rule_summary",
        "target_table": "quality_rule_summary",
        "expected_rows": 11,
        "description": "Data-quality rule summary.",
    },
]

SUPPORT_VIEW_SPECS: List[Dict[str, str]] = [
    {
        "view_name": "vw_support_pipeline_health",
        "description": "Pipeline health support view.",
    },
    {
        "view_name": "vw_support_exception_backlog",
        "description": "Exception backlog support view.",
    },
    {
        "view_name": "vw_support_exception_summary_daily",
        "description": "Daily exception summary support view.",
    },
    {
        "view_name": "vw_support_reconciliation_status",
        "description": "Source-to-target reconciliation support view.",
    },
    {
        "view_name": "vw_support_ticket_rca",
        "description": "Support ticket and RCA context view.",
    },
    {
        "view_name": "vw_support_data_reliability_overview",
        "description": "Data reliability overview support view.",
    },
]

# Source-to-target column aliases where Gold column names may not exactly match
# the Azure SQL support mart column names.
COLUMN_ALIASES: Dict[str, Dict[str, List[str]]] = {
    "fact_pipeline_run": {
        "started_at": [
            "started_at",
            "run_started_at",
            "run_started_at_utc",
            "latest_run_started_at",
        ],
        "completed_at": [
            "completed_at",
            "run_completed_at",
            "run_completed_at_utc",
            "latest_run_completed_at",
        ],
        "status": [
            "status",
            "run_status",
            "latest_status",
        ],
        "error_message": [
            "error_message",
            "latest_error_message",
        ],
    },
    "pipeline_health_summary": {
        "latest_run_started_at": [
            "latest_run_started_at",
            "started_at",
            "run_started_at",
            "run_started_at_utc",
        ],
        "latest_run_completed_at": [
            "latest_run_completed_at",
            "completed_at",
            "run_completed_at",
            "run_completed_at_utc",
        ],
        "latest_error_message": [
            "latest_error_message",
            "error_message",
        ],
    },
    "fact_data_quality_exception": {
        "exception_status": [
            "exception_status",
            "status",
            "triage_status",
        ],
    },
    "exception_summary_daily": {
        "source_systems_text": [
            "source_systems_text",
            "source_systems",
        ],
        "polling_platforms_text": [
            "polling_platforms_text",
            "polling_platforms",
        ],
        "device_statuses_text": [
            "device_statuses_text",
            "device_statuses",
        ],
        "communication_statuses_text": [
            "communication_statuses_text",
            "communication_statuses",
        ],
        "signal_qualities_text": [
            "signal_qualities_text",
            "signal_qualities",
        ],
    },
    "fact_support_ticket": {
        "ticket_status": [
            "ticket_status",
            "status",
        ],
    },
    "quality_rule_summary": {
        "active_flag": [
            "active_flag",
            "is_active",
            "active",
        ],
    },
}


# -----------------------------------------------------------------------------
# Identifier and SQL Server helpers
# -----------------------------------------------------------------------------

def is_safe_identifier(identifier: str) -> bool:
    """
    Return whether identifier is a safe SQL identifier.
    """
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier))


def require_safe_identifier(identifier: str, label: str) -> None:
    """
    Raise if an identifier is unsafe.
    """
    if not is_safe_identifier(identifier):
        raise ValueError(f"Unsafe {label}: {identifier}")


def qualified_table_name(schema_name: str, table_name: str) -> str:
    """
    Build schema.table name after identifier validation.
    """
    require_safe_identifier(schema_name, "schema name")
    require_safe_identifier(table_name, "table name")

    return f"{schema_name}.{table_name}"


def read_sqlserver_query(sql_query: str) -> DataFrame:
    """
    Read a SQL query from Azure SQL using Databricks SQL Server data source.

    Uses the query option instead of wrapping SQL manually as a dbtable derived
    table. This avoids SQL Server ORDER BY restrictions inside wrapped subqueries.
    """
    clean_query = sql_query.strip().rstrip(";")

    return (
        base_sqlserver_reader()
        .option("query", clean_query)
        .load()
    )


def read_sqlserver_table(table_name: str) -> DataFrame:
    """
    Read a table or view from Azure SQL using Databricks SQL Server data source.
    """
    require_safe_identifier(table_name, "table/view name")

    return (
        base_sqlserver_reader()
        .option("dbtable", qualified_table_name(AZURE_SQL_SCHEMA, table_name))
        .load()
    )


def fetch_single_value(sql_query: str) -> Any:
    """
    Execute a SELECT query through SQL Server data source and return the first value.
    """
    rows = read_sqlserver_query(sql_query).collect()

    if not rows:
        return None

    return rows[0][0]


def azure_sql_table_exists(table_name: str) -> bool:
    """
    Return whether a target Azure SQL table exists.
    """
    require_safe_identifier(table_name, "table name")

    query = f"""
SELECT
    COUNT(*) AS object_count
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '{AZURE_SQL_SCHEMA}'
  AND TABLE_NAME = '{table_name}'
  AND TABLE_TYPE = 'BASE TABLE'
"""

    value = fetch_single_value(query)
    return int(value or 0) > 0


def count_azure_sql_object(object_name: str) -> int:
    """
    Count rows in an Azure SQL table or view.
    """
    require_safe_identifier(object_name, "table/view name")

    query = (
        f"SELECT COUNT(*) AS row_count "
        f"FROM {qualified_table_name(AZURE_SQL_SCHEMA, object_name)}"
    )

    value = fetch_single_value(query)
    return int(value or 0)


def get_azure_sql_target_columns(table_name: str) -> List[Dict[str, Any]]:
    """
    Return target table column metadata from Azure SQL.

    Do not use ORDER BY in the SQL query here. Sort after collect.
    """
    require_safe_identifier(table_name, "table name")

    query = f"""
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    CHARACTER_MAXIMUM_LENGTH,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '{AZURE_SQL_SCHEMA}'
  AND TABLE_NAME = '{table_name}'
"""

    column_df = read_sqlserver_query(query)

    column_rows = sorted(
        column_df.collect(),
        key=lambda row: int(row["ORDINAL_POSITION"]),
    )

    columns: List[Dict[str, Any]] = []

    for row in column_rows:
        columns.append(
            {
                "column_name": row["COLUMN_NAME"],
                "data_type": row["DATA_TYPE"],
                "numeric_precision": row["NUMERIC_PRECISION"],
                "numeric_scale": row["NUMERIC_SCALE"],
                "character_maximum_length": row["CHARACTER_MAXIMUM_LENGTH"],
                "ordinal_position": row["ORDINAL_POSITION"],
            }
        )

    if not columns:
        raise RuntimeError(
            f"No target columns found for Azure SQL table "
            f"{AZURE_SQL_SCHEMA}.{table_name}. "
            f"Run azure_sql/01_create_support_mart.sql first."
        )

    return columns


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


# -----------------------------------------------------------------------------
# Data alignment helpers
# -----------------------------------------------------------------------------

def cast_expression_for_sql_type(expr: Any, column_metadata: Dict[str, Any]) -> Any:
    """
    Cast Spark expression to a type compatible with the Azure SQL target column.
    """
    data_type = str(column_metadata.get("data_type", "")).lower()

    if data_type == "bigint":
        return expr.cast("long")

    if data_type in {"int", "smallint", "tinyint"}:
        return expr.cast("int")

    if data_type in {"decimal", "numeric"}:
        precision_raw = column_metadata.get("numeric_precision")
        scale_raw = column_metadata.get("numeric_scale")

        precision = int(precision_raw) if precision_raw is not None else 18
        scale = int(scale_raw) if scale_raw is not None else 6

        return expr.cast(DecimalType(precision, scale))

    if data_type in {"float", "real"}:
        return expr.cast("double")

    if data_type == "bit":
        return expr.cast("boolean")

    if data_type == "date":
        return expr.cast("date")

    if data_type in {"datetime", "datetime2", "smalldatetime", "datetimeoffset"}:
        return expr.cast("timestamp")

    # varchar, nvarchar, char, nchar, text, uniqueidentifier, and fallback.
    return expr.cast("string")


def get_source_column_expr(
    target_table: str,
    target_column: str,
    source_columns_by_lower: Dict[str, str],
) -> Any:
    """
    Return a Spark expression for a target column using source columns,
    configured aliases, or publish metadata.
    """
    target_column_lower = target_column.lower()

    alias_candidates = COLUMN_ALIASES.get(target_table, {}).get(
        target_column_lower,
        [target_column_lower],
    )

    # Always allow exact target column as a candidate.
    if target_column_lower not in alias_candidates:
        alias_candidates = [target_column_lower] + alias_candidates

    for candidate in alias_candidates:
        candidate_lower = candidate.lower()

        if candidate_lower in source_columns_by_lower:
            source_column = source_columns_by_lower[candidate_lower]
            return F.col(source_column)

    if target_column_lower == "_azure_sql_publish_run_id":
        return F.lit(PUBLISH_RUN_ID)

    if target_column_lower == "_azure_sql_published_at_utc":
        return F.lit(PUBLISHED_AT_UTC_SQL).cast("timestamp")

    return F.lit(None)


def align_dataframe_to_target_table(
    source_df: DataFrame,
    target_table: str,
    target_columns: List[Dict[str, Any]],
) -> DataFrame:
    """
    Align Spark DataFrame columns to the Azure SQL target table definition.
    """
    source_columns_by_lower = {
        column_name.lower(): column_name
        for column_name in source_df.columns
    }

    select_expressions = []

    for column_metadata in target_columns:
        target_column = column_metadata["column_name"]

        expr = get_source_column_expr(
            target_table=target_table,
            target_column=target_column,
            source_columns_by_lower=source_columns_by_lower,
        )

        casted_expr = cast_expression_for_sql_type(
            expr=expr,
            column_metadata=column_metadata,
        ).alias(target_column)

        select_expressions.append(casted_expr)

    return source_df.select(*select_expressions)


# -----------------------------------------------------------------------------
# Write helpers
# -----------------------------------------------------------------------------

def write_dataframe_to_azure_sql(
    df: DataFrame,
    target_table: str,
    truncate_existing: bool = True,
) -> int:
    """
    Write a Spark DataFrame to Azure SQL using the Databricks SQL Server data source.

    Existing setup tables are preserved and loaded with overwrite + truncate.
    The truncate option avoids dropping tables that already have support views.
    """
    require_safe_identifier(target_table, "target table")

    writer = (
        base_sqlserver_writer(df)
        .option("dbtable", qualified_table_name(AZURE_SQL_SCHEMA, target_table))
    )

    if truncate_existing:
        writer = writer.option("truncate", "true")

    writer.mode("overwrite").save()

    return count_azure_sql_object(target_table)


def publish_gold_table_to_azure_sql(table_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Publish a single Databricks Gold table into Azure SQL.
    """
    source_table_name = table_spec["source_table"]
    target_table_name = table_spec["target_table"]
    expected_rows = int(table_spec["expected_rows"])
    description = table_spec["description"]

    source_fqn = f"{GOLD_SCHEMA}.{source_table_name}"

    print("-" * 90)
    print(f"Publishing source table: {source_fqn}")
    print(
        "Target Azure SQL table: "
        f"{AZURE_SQL_DATABASE}.{AZURE_SQL_SCHEMA}.{target_table_name}"
    )
    print(f"Description: {description}")
    print(f"Expected rows: {expected_rows:,}")

    if not spark_table_exists(source_fqn):
        raise RuntimeError(f"Required Gold source table not found: {source_fqn}")

    if not azure_sql_table_exists(target_table_name):
        raise RuntimeError(
            f"Target Azure SQL table not found: "
            f"{AZURE_SQL_SCHEMA}.{target_table_name}. "
            f"Run azure_sql/01_create_support_mart.sql first."
        )

    source_df = spark.table(source_fqn)  # type: ignore[name-defined]
    source_rows = source_df.count()

    print(f"Source rows: {source_rows:,}")

    target_columns = get_azure_sql_target_columns(target_table_name)

    aligned_df = align_dataframe_to_target_table(
        source_df=source_df,
        target_table=target_table_name,
        target_columns=target_columns,
    )

    written_rows = write_dataframe_to_azure_sql(
        df=aligned_df,
        target_table=target_table_name,
        truncate_existing=True,
    )

    if source_rows == expected_rows and written_rows == expected_rows:
        status = "PASS"
    elif source_rows == written_rows:
        status = "WARN_EXPECTED_COUNT_CHANGED"
    else:
        status = "FAIL"

    print(f"Written rows: {written_rows:,}")
    print(f"Status: {status}")

    return {
        "publish_run_id": PUBLISH_RUN_ID,
        "source_table": source_fqn,
        "target_table": (
            f"{AZURE_SQL_DATABASE}."
            f"{AZURE_SQL_SCHEMA}."
            f"{target_table_name}"
        ),
        "description": description,
        "expected_rows": expected_rows,
        "source_rows": int(source_rows),
        "written_rows": int(written_rows),
        "status": status,
        "published_at_utc": PUBLISHED_AT_UTC_SQL,
    }


def build_summary_dataframe(summary_rows: List[Dict[str, Any]]) -> DataFrame:
    """
    Build a Spark DataFrame from summary dictionaries.
    """
    summary_pdf = pd.DataFrame(summary_rows)

    if summary_pdf.empty:
        raise RuntimeError("Cannot build summary dataframe from empty rows.")

    return spark.createDataFrame(summary_pdf)  # type: ignore[name-defined]


def validate_support_views() -> List[Dict[str, Any]]:
    """
    Validate Azure SQL support views and return summary rows.
    """
    view_rows: List[Dict[str, Any]] = []

    print("=" * 90)
    print("Validating Azure SQL support views...")

    for view_spec in SUPPORT_VIEW_SPECS:
        view_name = view_spec["view_name"]
        description = view_spec["description"]

        require_safe_identifier(view_name, "view name")

        print("-" * 90)
        print(f"Validating view: {AZURE_SQL_DATABASE}.{AZURE_SQL_SCHEMA}.{view_name}")

        row_count = count_azure_sql_object(view_name)

        print(f"View rows: {row_count:,}")

        view_rows.append(
            {
                "publish_run_id": PUBLISH_RUN_ID,
                "view_name": view_name,
                "description": description,
                "view_rows": int(row_count),
                "status": "PASS",
                "published_at_utc": PUBLISHED_AT_UTC_SQL,
            }
        )

    return view_rows


# -----------------------------------------------------------------------------
# Start publish
# -----------------------------------------------------------------------------

print("=" * 90)
print("MeterFlow IQ - Publish Databricks Gold to Azure SQL Support Mart")
print("=" * 90)
print(f"Spark version: {spark.version}")  # type: ignore[name-defined]
print(f"Publish run ID: {PUBLISH_RUN_ID}")
print(f"Published at UTC: {PUBLISHED_AT_UTC_SQL}")
print(f"Databricks Gold schema: {GOLD_SCHEMA}")
print(f"Azure SQL server: {AZURE_SQL_SERVER}")
print(f"Azure SQL database: {AZURE_SQL_DATABASE}")
print(f"Azure SQL schema: {AZURE_SQL_SCHEMA}")
print(f"Azure SQL user: {AZURE_SQL_USER}")
print("Azure SQL password: [not printed]")


# -----------------------------------------------------------------------------
# Validate source Gold tables
# -----------------------------------------------------------------------------

print("=" * 90)
print("Validating Databricks Gold source tables...")

for spec in SUPPORT_TABLE_SPECS:
    source_fqn = f"{GOLD_SCHEMA}.{spec['source_table']}"

    if not spark_table_exists(source_fqn):
        raise RuntimeError(f"Required Gold source table not found: {source_fqn}")

    print(f"Found source table: {source_fqn}")

print("All required Gold source tables found.")


# -----------------------------------------------------------------------------
# Validate Azure SQL connection
# -----------------------------------------------------------------------------

print("=" * 90)
print("Testing Azure SQL connection with Databricks SQL Server data source...")

try:
    connection_test_df = read_sqlserver_query(
        """
SELECT
    DB_NAME() AS current_database,
    SUSER_SNAME() AS login_name,
    @@SERVERNAME AS server_name,
    SYSDATETIMEOFFSET() AS checked_at
"""
    )

    connection_test_rows = connection_test_df.collect()

    if not connection_test_rows:
        raise RuntimeError("Azure SQL connection test returned no rows.")

    connection_test_row = connection_test_rows[0]

    print("Azure SQL connection test:")
    print(f" - current_database: {connection_test_row['current_database']}")
    print(f" - login_name: {connection_test_row['login_name']}")
    print(f" - server_name: {connection_test_row['server_name']}")
    print(f" - checked_at: {connection_test_row['checked_at']}")

except Exception as exc:
    print("=" * 90)
    print("Azure SQL connection failed.")
    print("Common causes:")
    print(" - Azure SQL firewall does not allow the Databricks outbound IP.")
    print(" - Azure SQL password is wrong.")
    print(" - Azure SQL server/database name is wrong.")
    print(" - Azure SQL free/serverless database is waking up; wait and retry.")
    print(" - Databricks SQL Server data source options are unsupported in this runtime.")
    print("=" * 90)
    raise exc


# -----------------------------------------------------------------------------
# Publish support mart tables
# -----------------------------------------------------------------------------

summary_rows: List[Dict[str, Any]] = []

print("=" * 90)
print("Publishing technical/support tables to Azure SQL...")

for spec in SUPPORT_TABLE_SPECS:
    summary_row = publish_gold_table_to_azure_sql(spec)
    summary_rows.append(summary_row)


# -----------------------------------------------------------------------------
# Write publish summary table
# -----------------------------------------------------------------------------

print("=" * 90)
print("Writing Azure SQL publish summary table...")

summary_df = build_summary_dataframe(summary_rows)

summary_written_rows = write_dataframe_to_azure_sql(
    df=summary_df,
    target_table="_databricks_azure_sql_publish_summary",
    truncate_existing=False,
)

print("Publish summary table written:")
print(
    " - table: "
    f"{AZURE_SQL_DATABASE}."
    f"{AZURE_SQL_SCHEMA}."
    "_databricks_azure_sql_publish_summary"
)
print(f" - rows: {summary_written_rows:,}")


# -----------------------------------------------------------------------------
# Validate views and write view summary table
# -----------------------------------------------------------------------------

view_summary_rows = validate_support_views()

view_summary_df = build_summary_dataframe(view_summary_rows)

view_summary_written_rows = write_dataframe_to_azure_sql(
    df=view_summary_df,
    target_table="_databricks_azure_sql_view_summary",
    truncate_existing=False,
)

print("View summary table written:")
print(
    " - table: "
    f"{AZURE_SQL_DATABASE}."
    f"{AZURE_SQL_SCHEMA}."
    "_databricks_azure_sql_view_summary"
)
print(f" - rows: {view_summary_written_rows:,}")


# -----------------------------------------------------------------------------
# Final validation
# -----------------------------------------------------------------------------

print("=" * 90)
print("Azure SQL publish summary:")
for row in summary_rows:
    print(row)

non_pass_rows = [
    row
    for row in summary_rows
    if row["status"] != "PASS"
]

if non_pass_rows:
    print("=" * 90)
    print("WARNING: Some Azure SQL publish checks did not return PASS:")
    for row in non_pass_rows:
        print(row)

    raise RuntimeError(
        "One or more Azure SQL publish checks failed or changed expected counts."
    )

print("=" * 90)
print("All fixed-count Azure SQL support publish checks passed.")

print("=" * 90)
print("Created / updated Azure SQL support tables:")
for row in summary_rows:
    print(f" - {row['target_table']} ({row['written_rows']:,} rows)")

print("=" * 90)
print("Validated Azure SQL support views:")
for row in view_summary_rows:
    print(
        f" - {AZURE_SQL_DATABASE}."
        f"{AZURE_SQL_SCHEMA}."
        f"{row['view_name']} "
        f"({row['view_rows']:,} rows)"
    )

print("=" * 90)
print("Azure SQL Support publish complete.")
print("Status: PASS")
print(f"Publish run ID: {PUBLISH_RUN_ID}")


# -----------------------------------------------------------------------------
# Display summaries in Databricks output
# -----------------------------------------------------------------------------

if summary_rows:
    safe_display_dataframe(pd.DataFrame(summary_rows))

if view_summary_rows:
    safe_display_dataframe(pd.DataFrame(view_summary_rows))