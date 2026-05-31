"""
MeterFlow IQ - Streamlit BigQuery Connection Check

Temporary local test to confirm Streamlit can query BigQuery
using the published MeterFlow IQ views.
"""

import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.credentials import Credentials


load_dotenv()

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "").strip()
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "").strip()
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US").strip()
GCP_ACCESS_TOKEN = os.getenv("GCP_ACCESS_TOKEN", "").strip()


st.set_page_config(
    page_title="MeterFlow IQ - BigQuery Connection Check",
    page_icon="📊",
    layout="wide",
)

st.title("MeterFlow IQ - BigQuery Connection Check")

missing_values = []

if not BIGQUERY_PROJECT_ID:
    missing_values.append("BIGQUERY_PROJECT_ID")

if not BIGQUERY_DATASET_ID:
    missing_values.append("BIGQUERY_DATASET_ID")

if not BIGQUERY_LOCATION:
    missing_values.append("BIGQUERY_LOCATION")

if not GCP_ACCESS_TOKEN:
    missing_values.append("GCP_ACCESS_TOKEN")

if missing_values:
    st.error(
        "Missing required environment values: "
        + ", ".join(missing_values)
    )
    st.stop()


credentials = Credentials(
    token=GCP_ACCESS_TOKEN,
    quota_project_id=BIGQUERY_PROJECT_ID,
)

client = bigquery.Client(
    project=BIGQUERY_PROJECT_ID,
    credentials=credentials,
    location=BIGQUERY_LOCATION,
)


def run_query(sql: str) -> pd.DataFrame:
    query_job = client.query(sql, location=BIGQUERY_LOCATION)
    return query_job.result().to_dataframe()


st.info(
    f"Project: `{BIGQUERY_PROJECT_ID}` | "
    f"Dataset: `{BIGQUERY_DATASET_ID}` | "
    f"Location: `{BIGQUERY_LOCATION}`"
)

test_sql = f"""
SELECT
  pipeline_name,
  latest_status,
  run_count,
  success_count,
  failed_count,
  failure_rate
FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.vw_streamlit_pipeline_health`
ORDER BY pipeline_name
"""

try:
    df = run_query(test_sql)

    st.success("BigQuery connection successful.")
    st.subheader("Pipeline Health Sample")
    st.dataframe(df, use_container_width=True)

    st.metric("Rows returned", len(df))

except Exception as exc:
    st.error("BigQuery query failed.")
    st.exception(exc)