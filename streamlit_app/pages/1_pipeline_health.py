"""
MeterFlow IQ - Pipeline Health Page

Shows pipeline health and operational reliability metrics from BigQuery.

Uses:
- vw_streamlit_pipeline_health
- vw_streamlit_reconciliation
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# Make streamlit_app/utils importable from this pages folder.
APP_DIR = Path(__file__).resolve().parents[1]

if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from utils.bigquery_client import (
    clear_streamlit_caches,
    fq_table,
    get_environment_label,
    run_query,
)


st.set_page_config(
    page_title="MeterFlow IQ - Pipeline Health",
    page_icon="🩺",
    layout="wide",
)

st.title("Pipeline Health")
st.caption(
    "Latest run status, failures, partial loads, rejected rows, "
    "and source-to-target reliability context."
)

st.info(get_environment_label())


with st.sidebar:
    st.header("Pipeline Health")

    if st.button("Refresh BigQuery cache"):
        clear_streamlit_caches()
        st.rerun()

    st.caption(
        "If BigQuery authentication fails, refresh `GCP_ACCESS_TOKEN` "
        "in `.env`, stop Streamlit, and rerun the app."
    )


# -----------------------------------------------------------------------------
# Load BigQuery data
# -----------------------------------------------------------------------------

pipeline_sql = f"""
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
FROM {fq_table("vw_streamlit_pipeline_health")}
ORDER BY pipeline_name
"""

reconciliation_sql = f"""
SELECT
  stage_order,
  layer,
  object_name,
  row_count,
  metric_type,
  notes,
  _bigquery_publish_run_id,
  _bigquery_published_at_utc
FROM {fq_table("vw_streamlit_reconciliation")}
ORDER BY stage_order
"""

try:
    pipeline_df = run_query(pipeline_sql)
    reconciliation_df = run_query(reconciliation_sql)
except Exception as exc:
    st.error("BigQuery query failed.")
    st.exception(exc)
    st.stop()


if pipeline_df.empty:
    st.warning("No pipeline health rows returned from BigQuery.")
    st.stop()


# -----------------------------------------------------------------------------
# Normalize data types for display/charts
# -----------------------------------------------------------------------------

numeric_pipeline_columns = [
    "latest_duration_minutes",
    "latest_rows_read",
    "latest_rows_accepted",
    "latest_rows_rejected",
    "run_count",
    "success_count",
    "failed_count",
    "partial_load_count",
    "runs_with_errors_count",
    "failure_rate",
    "overall_rejected_rate",
]

for column_name in numeric_pipeline_columns:
    if column_name in pipeline_df.columns:
        pipeline_df[column_name] = pd.to_numeric(
            pipeline_df[column_name],
            errors="coerce",
        )

datetime_pipeline_columns = [
    "latest_run_started_at",
    "latest_run_completed_at",
    "_bigquery_published_at_utc",
]

for column_name in datetime_pipeline_columns:
    if column_name in pipeline_df.columns:
        pipeline_df[column_name] = pd.to_datetime(
            pipeline_df[column_name],
            errors="coerce",
        )

if not reconciliation_df.empty:
    if "row_count" in reconciliation_df.columns:
        reconciliation_df["row_count"] = pd.to_numeric(
            reconciliation_df["row_count"],
            errors="coerce",
        )

    if "_bigquery_published_at_utc" in reconciliation_df.columns:
        reconciliation_df["_bigquery_published_at_utc"] = pd.to_datetime(
            reconciliation_df["_bigquery_published_at_utc"],
            errors="coerce",
        )


# -----------------------------------------------------------------------------
# Filters
# -----------------------------------------------------------------------------

status_options = sorted(
    pipeline_df["latest_status"]
    .dropna()
    .astype(str)
    .unique()
    .tolist()
)

selected_statuses = st.sidebar.multiselect(
    "Latest status",
    options=status_options,
    default=status_options,
)

filtered_df = pipeline_df.copy()

if selected_statuses:
    filtered_df = filtered_df[
        filtered_df["latest_status"].isin(selected_statuses)
    ]

if filtered_df.empty:
    st.warning("No pipeline rows match the selected filters.")
    st.stop()


# -----------------------------------------------------------------------------
# KPI cards
# -----------------------------------------------------------------------------

total_pipelines = len(filtered_df)
total_runs = int(filtered_df["run_count"].fillna(0).sum())
total_failures = int(filtered_df["failed_count"].fillna(0).sum())
total_partial_loads = int(filtered_df["partial_load_count"].fillna(0).sum())
total_latest_rejected_rows = int(filtered_df["latest_rows_rejected"].fillna(0).sum())

if total_runs > 0:
    overall_failure_rate = total_failures / total_runs
else:
    overall_failure_rate = 0.0

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.metric("Pipelines", f"{total_pipelines:,}")

with kpi2:
    st.metric("Total Runs", f"{total_runs:,}")

with kpi3:
    st.metric("Failures", f"{total_failures:,}")

with kpi4:
    st.metric("Partial Loads", f"{total_partial_loads:,}")

with kpi5:
    st.metric("Latest Rejected Rows", f"{total_latest_rejected_rows:,}")

st.caption(f"Overall failure rate across selected pipelines: **{overall_failure_rate:.2%}**")


# -----------------------------------------------------------------------------
# Charts
# -----------------------------------------------------------------------------

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Latest Status Distribution")

    status_count_df = (
        filtered_df
        .groupby("latest_status", dropna=False)
        .size()
        .reset_index(name="pipeline_count")
    )

    fig_status = px.bar(
        status_count_df,
        x="latest_status",
        y="pipeline_count",
        text="pipeline_count",
        title="Pipeline Count by Latest Status",
    )

    st.plotly_chart(fig_status, use_container_width=True)

with chart_col2:
    st.subheader("Failures by Pipeline")

    failure_df = filtered_df.sort_values(
        by=["failed_count", "partial_load_count"],
        ascending=False,
    )

    fig_failures = px.bar(
        failure_df,
        x="pipeline_name",
        y="failed_count",
        title="Failure Count by Pipeline",
        hover_data=[
            "latest_status",
            "partial_load_count",
            "runs_with_errors_count",
            "failure_rate",
            "overall_rejected_rate",
        ],
    )

    st.plotly_chart(fig_failures, use_container_width=True)


# -----------------------------------------------------------------------------
# Pipeline detail table
# -----------------------------------------------------------------------------

st.subheader("Pipeline Health Detail")

pipeline_display_columns = [
    "pipeline_name",
    "latest_status",
    "latest_run_started_at",
    "latest_run_completed_at",
    "latest_duration_minutes",
    "latest_rows_read",
    "latest_rows_accepted",
    "latest_rows_rejected",
    "run_count",
    "success_count",
    "failed_count",
    "partial_load_count",
    "runs_with_errors_count",
    "failure_rate",
    "overall_rejected_rate",
    "latest_error_message",
]

available_pipeline_display_columns = [
    column_name
    for column_name in pipeline_display_columns
    if column_name in filtered_df.columns
]

st.dataframe(
    filtered_df[available_pipeline_display_columns],
    use_container_width=True,
    hide_index=True,
)


# -----------------------------------------------------------------------------
# Source-to-target reconciliation
# -----------------------------------------------------------------------------

st.subheader("Source-to-Target Reconciliation Snapshot")

if reconciliation_df.empty:
    st.warning("No source-to-target reconciliation rows returned from BigQuery.")
else:
    rec_col1, rec_col2 = st.columns([1.2, 1])

    with rec_col1:
        reconciliation_display_columns = [
            "stage_order",
            "layer",
            "object_name",
            "row_count",
            "metric_type",
            "notes",
        ]

        available_reconciliation_display_columns = [
            column_name
            for column_name in reconciliation_display_columns
            if column_name in reconciliation_df.columns
        ]

        st.dataframe(
            reconciliation_df[available_reconciliation_display_columns],
            use_container_width=True,
            hide_index=True,
        )

    with rec_col2:
        fig_reconciliation = px.line(
            reconciliation_df,
            x="stage_order",
            y="row_count",
            markers=True,
            title="Row Count by Pipeline Stage",
            hover_data=[
                "layer",
                "object_name",
                "metric_type",
                "notes",
            ],
        )

        st.plotly_chart(fig_reconciliation, use_container_width=True)


# -----------------------------------------------------------------------------
# Publish metadata
# -----------------------------------------------------------------------------

with st.expander("BigQuery publish metadata"):
    metadata_columns = [
        "_bigquery_publish_run_id",
        "_bigquery_published_at_utc",
    ]

    available_metadata_columns = [
        column_name
        for column_name in metadata_columns
        if column_name in filtered_df.columns
    ]

    if available_metadata_columns:
        st.write("Pipeline health publish metadata")
        st.dataframe(
            filtered_df[available_metadata_columns].drop_duplicates(),
            use_container_width=True,
            hide_index=True,
        )

    if not reconciliation_df.empty:
        available_reconciliation_metadata_columns = [
            column_name
            for column_name in metadata_columns
            if column_name in reconciliation_df.columns
        ]

        if available_reconciliation_metadata_columns:
            st.write("Reconciliation publish metadata")
            st.dataframe(
                reconciliation_df[
                    available_reconciliation_metadata_columns
                ].drop_duplicates(),
                use_container_width=True,
                hide_index=True,
            )