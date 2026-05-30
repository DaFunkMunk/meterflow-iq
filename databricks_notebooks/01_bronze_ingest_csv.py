"""
MeterFlow IQ - 01 Bronze Ingest CSV

Purpose:
Read the nine structured source CSV files from sample_data/,
add Bronze audit metadata, write managed Delta tables,
and validate row counts.

Bronze rule:
Do not clean or fix records here. Bronze preserves raw source records
with traceability metadata.
"""

from pathlib import Path
import os
import uuid

from pyspark.sql import functions as F

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

BRONZE_SCHEMA = "meterflow_iq_bronze"
BRONZE_RUN_ID = f"bronze_csv_{uuid.uuid4().hex[:12]}"

CSV_SOURCES = {
    "facility_master.csv": {
        "target_table": "bronze_facility_master",
        "expected_rows": 25,
    },
    "meter_master.csv": {
        "target_table": "bronze_meter_master",
        "expected_rows": 125,
    },
    "raw_polling_readings.csv": {
        "target_table": "bronze_raw_polling_readings",
        "expected_rows": 180_056,
    },
    "flowcal_measurement_extract.csv": {
        "target_table": "bronze_flowcal_measurement_extract",
        "expected_rows": 7_500,
    },
    "nominations_daily.csv": {
        "target_table": "bronze_nominations_daily",
        "expected_rows": 1_500,
    },
    "support_tickets.csv": {
        "target_table": "bronze_support_tickets",
        "expected_rows": 350,
    },
    "pipeline_run_log.csv": {
        "target_table": "bronze_pipeline_run_log",
        "expected_rows": 150,
    },
    "dq_rules_reference.csv": {
        "target_table": "bronze_dq_rules_reference",
        "expected_rows": 20,
    },
    "known_issue_scenarios.csv": {
        "target_table": "bronze_known_issue_scenarios",
        "expected_rows": 15,
    },
}

# -----------------------------------------------------------------------------
# Locate sample_data folder
# -----------------------------------------------------------------------------

print("Spark version:", spark.version)
print("Current working directory:", os.getcwd())
print("Bronze run ID:", BRONZE_RUN_ID)

cwd = Path(os.getcwd())

candidate_sample_dirs = [
    cwd / "sample_data",
    cwd.parent / "sample_data",
    Path("/Workspace/Users/ngpend2@gmail.com/meterflow-iq/sample_data"),
]

sample_dir = None

for path in candidate_sample_dirs:
    exists = path.exists()
    has_facility = (path / "facility_master.csv").exists()
    print(f"Checking: {path} | exists={exists} | has_facility_master={has_facility}")

    if exists and has_facility:
        sample_dir = path
        break

if sample_dir is None:
    raise RuntimeError("Could not find sample_data/facility_master.csv from this notebook.")

print(f"Using sample_data folder: {sample_dir}")

# -----------------------------------------------------------------------------
# Create Bronze schema/database
# -----------------------------------------------------------------------------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"USE {BRONZE_SCHEMA}")

print(f"Using Bronze schema/database: {BRONZE_SCHEMA}")

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def add_bronze_metadata(df, source_file: str, source_path: str):
    """
    Add standard Bronze metadata columns.

    These columns help prove source-to-target lineage later.
    """
    original_columns = df.columns

    record_hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(col_name).cast("string"), F.lit(""))
                for col_name in original_columns
            ],
        ),
        256,
    )

    return (
        df
        .withColumn("_bronze_run_id", F.lit(BRONZE_RUN_ID))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_path", F.lit(source_path))
        .withColumn("_source_type", F.lit("csv"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", record_hash_expr)
    )

def read_csv(source_file: str):
    source_path = sample_dir / source_file
    spark_path = f"file:{source_path}"

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", False)
        .option("escape", '"')
        .csv(spark_path)
    )

# -----------------------------------------------------------------------------
# Ingest CSV files into Bronze Delta tables
# -----------------------------------------------------------------------------

summary_rows = []

for source_file, config in CSV_SOURCES.items():
    target_table = config["target_table"]
    expected_rows = config["expected_rows"]
    source_path = sample_dir / source_file

    print("-" * 90)
    print(f"Reading source file: {source_file}")
    print(f"Target Bronze table: {BRONZE_SCHEMA}.{target_table}")

    if not source_path.exists():
        raise FileNotFoundError(f"Missing source file: {source_path}")

    raw_df = read_csv(source_file)
    actual_rows = raw_df.count()

    bronze_df = add_bronze_metadata(
        df=raw_df,
        source_file=source_file,
        source_path=str(source_path),
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{BRONZE_SCHEMA}.{target_table}")
    )

    written_rows = spark.table(f"{BRONZE_SCHEMA}.{target_table}").count()

    status = "PASS" if actual_rows == expected_rows and written_rows == expected_rows else "CHECK"

    summary_rows.append(
        {
            "source_file": source_file,
            "target_table": f"{BRONZE_SCHEMA}.{target_table}",
            "expected_rows": expected_rows,
            "source_rows": actual_rows,
            "written_rows": written_rows,
            "status": status,
        }
    )

    print(f"Expected rows: {expected_rows:,}")
    print(f"Source rows:   {actual_rows:,}")
    print(f"Written rows:  {written_rows:,}")
    print(f"Status:        {status}")

# -----------------------------------------------------------------------------
# Display validation summary
# -----------------------------------------------------------------------------

summary_df = spark.createDataFrame(summary_rows)

print("=" * 90)
print("Bronze CSV ingest complete.")
print(f"Bronze schema/database: {BRONZE_SCHEMA}")
print(f"Bronze run ID: {BRONZE_RUN_ID}")

display(summary_df.orderBy("source_file"))

failed_df = summary_df.filter(F.col("status") != "PASS")
failed_count = failed_df.count()

if failed_count > 0:
    print("WARNING: One or more Bronze ingest row-count checks need review.")
    display(failed_df)
else:
    print("All Bronze CSV row-count checks passed.")

# -----------------------------------------------------------------------------
# Show created Bronze tables
# -----------------------------------------------------------------------------

print("Created Bronze tables:")
display(spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}"))