"""
MeterFlow IQ - Reconciliation Page

Shows source-to-target row-count checkpoints and BigQuery publish validation.

Uses:
- vw_streamlit_reconciliation
- _databricks_publish_summary
- vw_streamlit_pipeline_health

Purpose:
Help support/data analysts answer:
- Where did row counts change?
- Which changes are expected grain changes?
- Did BigQuery receive the same row counts Databricks published?
- Are the latest publish checks passing?
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

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
    get_bigquery_auth_caption,
    get_environment_label,
    run_query,
)


st.set_page_config(
    page_title="MeterFlow IQ - Reconciliation",
    page_icon="🔁",
    layout="wide",
)

st.title("Source-to-Target Reconciliation")
st.caption(
    "Trace row counts from source records through Bronze, Silver, Gold, "
    "and BigQuery publish validation."
)

st.info(get_environment_label())

st.warning(
    "Interpretation note: not every row-count change is data loss. "
    "Some stages intentionally change grain, such as hourly readings becoming meter-day "
    "or facility-day facts."
)


with st.sidebar:
    st.header("Reconciliation Filters")

    if st.button("Refresh BigQuery cache"):
        clear_streamlit_caches()
        st.rerun()

    st.caption(get_bigquery_auth_caption())


# -----------------------------------------------------------------------------
# Load BigQuery data
# -----------------------------------------------------------------------------

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

publish_summary_sql = f"""
SELECT
  publish_run_id,
  source_table,
  target_table,
  description,
  expected_rows,
  source_rows,
  written_rows,
  status,
  published_at_utc
FROM {fq_table("_databricks_publish_summary")}
ORDER BY target_table
"""

pipeline_health_sql = f"""
SELECT
  pipeline_name,
  latest_status,
  latest_run_started_at,
  latest_run_completed_at,
  latest_rows_read,
  latest_rows_accepted,
  latest_rows_rejected,
  run_count,
  failed_count,
  partial_load_count,
  failure_rate,
  overall_rejected_rate
FROM {fq_table("vw_streamlit_pipeline_health")}
ORDER BY pipeline_name
"""

try:
    reconciliation_df = run_query(reconciliation_sql)
    publish_summary_df = run_query(publish_summary_sql)
    pipeline_health_df = run_query(pipeline_health_sql)
except Exception as exc:
    st.error("BigQuery query failed.")
    st.exception(exc)
    st.stop()


if reconciliation_df.empty:
    st.warning("No reconciliation rows returned from BigQuery.")
    st.stop()


# -----------------------------------------------------------------------------
# Normalize data types
# -----------------------------------------------------------------------------

if "stage_order" in reconciliation_df.columns:
    reconciliation_df["stage_order"] = pd.to_numeric(
        reconciliation_df["stage_order"],
        errors="coerce",
    )

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

if not publish_summary_df.empty:
    numeric_publish_columns = [
        "expected_rows",
        "source_rows",
        "written_rows",
    ]

    for column_name in numeric_publish_columns:
        if column_name in publish_summary_df.columns:
            publish_summary_df[column_name] = pd.to_numeric(
                publish_summary_df[column_name],
                errors="coerce",
            )

    if "published_at_utc" in publish_summary_df.columns:
        publish_summary_df["published_at_utc"] = pd.to_datetime(
            publish_summary_df["published_at_utc"],
            errors="coerce",
        )

if not pipeline_health_df.empty:
    numeric_pipeline_columns = [
        "latest_rows_read",
        "latest_rows_accepted",
        "latest_rows_rejected",
        "run_count",
        "failed_count",
        "partial_load_count",
        "failure_rate",
        "overall_rejected_rate",
    ]

    for column_name in numeric_pipeline_columns:
        if column_name in pipeline_health_df.columns:
            pipeline_health_df[column_name] = pd.to_numeric(
                pipeline_health_df[column_name],
                errors="coerce",
            )

    datetime_pipeline_columns = [
        "latest_run_started_at",
        "latest_run_completed_at",
    ]

    for column_name in datetime_pipeline_columns:
        if column_name in pipeline_health_df.columns:
            pipeline_health_df[column_name] = pd.to_datetime(
                pipeline_health_df[column_name],
                errors="coerce",
            )


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def get_string_options(df: pd.DataFrame, column_name: str) -> List[str]:
    """
    Return sorted non-null string options for a dataframe column.
    """
    if column_name not in df.columns:
        return []

    values = (
        df[column_name]
        .dropna()
        .astype(str)
        .str.strip()
    )

    values = values[values != ""]

    return sorted(values.unique().tolist())


def apply_multiselect_filter(
    df: pd.DataFrame,
    column_name: str,
    selected_values: List[str],
) -> pd.DataFrame:
    """
    Apply multiselect filtering. Empty selection means include all.
    """
    if not selected_values or column_name not in df.columns:
        return df

    return df[df[column_name].astype(str).isin(selected_values)]


def classify_reconciliation_change(metric_type: str, object_name: str) -> str:
    """
    Add a business interpretation for each reconciliation checkpoint.
    """
    metric_type_clean = str(metric_type or "").lower()
    object_name_clean = str(object_name or "").lower()

    if "raw_source" in metric_type_clean:
        return "Original generated source record count."

    if "bronze_raw" in metric_type_clean:
        return "Bronze landing count should match the raw structured source count."

    if "silver_enriched" in metric_type_clean:
        return "Silver enriched count should preserve the raw reading grain after standardization/enrichment."

    if "silver_valid" in metric_type_clean:
        return "Rows classified as valid or review. Lower than raw is expected when exceptions are separated."

    if "silver_exception" in metric_type_clean:
        return "Readings with one or more data-quality exceptions."

    if "gold_meter_day" in metric_type_clean:
        return "Expected grain change: hourly readings are aggregated to meter-day facts."

    if "gold_facility_day" in metric_type_clean:
        return "Expected grain change: meter-day facts are aggregated to facility-day KPIs."

    if "gold_exception_fact" in metric_type_clean:
        return "One row per failed DQ rule per affected reading. This can exceed unique affected readings."

    if "exception" in object_name_clean:
        return "Exception-focused output used for investigation and triage."

    return "Review metric type and grain before interpreting this count as loss or mismatch."


def format_int(value) -> str:
    """
    Format a numeric value as an integer string.
    """
    if value is None or pd.isna(value):
        return "N/A"

    return f"{int(value):,}"


def format_pct(value) -> str:
    """
    Format a numeric value as a percent string.
    """
    if value is None or pd.isna(value):
        return "N/A"

    return f"{value:.2%}"


# -----------------------------------------------------------------------------
# Sidebar filters
# -----------------------------------------------------------------------------

layer_options = get_string_options(reconciliation_df, "layer")
metric_type_options = get_string_options(reconciliation_df, "metric_type")

selected_layers = st.sidebar.multiselect(
    "Layer",
    options=layer_options,
    default=layer_options,
)

selected_metric_types = st.sidebar.multiselect(
    "Metric type",
    options=metric_type_options,
    default=metric_type_options,
)

object_search = st.sidebar.text_input(
    "Object search",
    value="",
    help="Optional partial search against object_name.",
).strip()

publish_status_options = get_string_options(publish_summary_df, "status")

selected_publish_statuses = st.sidebar.multiselect(
    "BigQuery publish status",
    options=publish_status_options,
    default=publish_status_options,
)


# -----------------------------------------------------------------------------
# Apply filters
# -----------------------------------------------------------------------------

filtered_reconciliation_df = reconciliation_df.copy()

filtered_reconciliation_df = apply_multiselect_filter(
    filtered_reconciliation_df,
    "layer",
    selected_layers,
)

filtered_reconciliation_df = apply_multiselect_filter(
    filtered_reconciliation_df,
    "metric_type",
    selected_metric_types,
)

if object_search:
    filtered_reconciliation_df = filtered_reconciliation_df[
        filtered_reconciliation_df["object_name"]
        .fillna("")
        .astype(str)
        .str.contains(object_search, case=False, na=False)
    ]

filtered_publish_summary_df = publish_summary_df.copy()

filtered_publish_summary_df = apply_multiselect_filter(
    filtered_publish_summary_df,
    "status",
    selected_publish_statuses,
)

if filtered_reconciliation_df.empty:
    st.warning("No reconciliation rows match the selected filters.")
    st.stop()


# -----------------------------------------------------------------------------
# Derived reconciliation metrics
# -----------------------------------------------------------------------------

filtered_reconciliation_df = filtered_reconciliation_df.sort_values("stage_order").copy()

filtered_reconciliation_df["previous_row_count"] = (
    filtered_reconciliation_df["row_count"].shift(1)
)

filtered_reconciliation_df["row_count_delta_from_previous"] = (
    filtered_reconciliation_df["row_count"]
    - filtered_reconciliation_df["previous_row_count"]
)

filtered_reconciliation_df["row_count_delta_pct_from_previous"] = (
    filtered_reconciliation_df["row_count_delta_from_previous"]
    / filtered_reconciliation_df["previous_row_count"].replace(0, pd.NA)
)

filtered_reconciliation_df["interpretation"] = filtered_reconciliation_df.apply(
    lambda row: classify_reconciliation_change(
        row.get("metric_type"),
        row.get("object_name"),
    ),
    axis=1,
)


# -----------------------------------------------------------------------------
# KPI cards
# -----------------------------------------------------------------------------

checkpoint_count = len(filtered_reconciliation_df)
layer_count = filtered_reconciliation_df["layer"].nunique()
max_row_count = filtered_reconciliation_df["row_count"].max()
min_row_count = filtered_reconciliation_df["row_count"].min()

if not filtered_publish_summary_df.empty:
    publish_target_count = len(filtered_publish_summary_df)
    publish_pass_count = int(
        filtered_publish_summary_df["status"]
        .fillna("")
        .astype(str)
        .str.upper()
        .eq("PASS")
        .sum()
    )
else:
    publish_target_count = 0
    publish_pass_count = 0

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.metric("Checkpoints", f"{checkpoint_count:,}")

with kpi2:
    st.metric("Layers", f"{layer_count:,}")

with kpi3:
    st.metric("Max Row Count", format_int(max_row_count))

with kpi4:
    st.metric("Min Row Count", format_int(min_row_count))

with kpi5:
    st.metric("BigQuery PASS Targets", f"{publish_pass_count:,} / {publish_target_count:,}")


# -----------------------------------------------------------------------------
# Reconciliation charts
# -----------------------------------------------------------------------------

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Row Count by Pipeline Stage")

    fig_stage_line = px.line(
        filtered_reconciliation_df,
        x="stage_order",
        y="row_count",
        color="layer",
        markers=True,
        title="Source-to-Target Row Count Checkpoints",
        hover_data=[
            "object_name",
            "metric_type",
            "notes",
            "interpretation",
        ],
    )

    st.plotly_chart(fig_stage_line, use_container_width=True)

with chart_col2:
    st.subheader("Row Count by Object")

    fig_object_bar = px.bar(
        filtered_reconciliation_df,
        x="object_name",
        y="row_count",
        color="layer",
        title="Row Count by Reconciled Object",
        hover_data=[
            "stage_order",
            "metric_type",
            "notes",
            "interpretation",
        ],
    )

    st.plotly_chart(fig_object_bar, use_container_width=True)


# -----------------------------------------------------------------------------
# Reconciliation table
# -----------------------------------------------------------------------------

st.subheader("Source-to-Target Checkpoint Detail")

reconciliation_display_columns = [
    "stage_order",
    "layer",
    "object_name",
    "row_count",
    "previous_row_count",
    "row_count_delta_from_previous",
    "row_count_delta_pct_from_previous",
    "metric_type",
    "notes",
    "interpretation",
]

available_reconciliation_display_columns = [
    column_name
    for column_name in reconciliation_display_columns
    if column_name in filtered_reconciliation_df.columns
]

st.dataframe(
    filtered_reconciliation_df[available_reconciliation_display_columns],
    use_container_width=True,
    hide_index=True,
)


# -----------------------------------------------------------------------------
# Layer summary
# -----------------------------------------------------------------------------

st.subheader("Layer Summary")

layer_summary_df = (
    filtered_reconciliation_df
    .groupby("layer", dropna=False)
    .agg(
        checkpoint_count=("object_name", "count"),
        min_row_count=("row_count", "min"),
        max_row_count=("row_count", "max"),
        first_stage=("stage_order", "min"),
        last_stage=("stage_order", "max"),
    )
    .reset_index()
    .sort_values("first_stage")
)

summary_col1, summary_col2 = st.columns([1.1, 1])

with summary_col1:
    st.dataframe(
        layer_summary_df,
        use_container_width=True,
        hide_index=True,
    )

with summary_col2:
    fig_layer_summary = px.bar(
        layer_summary_df,
        x="layer",
        y="checkpoint_count",
        text="checkpoint_count",
        title="Checkpoint Count by Layer",
        hover_data=[
            "min_row_count",
            "max_row_count",
            "first_stage",
            "last_stage",
        ],
    )

    st.plotly_chart(fig_layer_summary, use_container_width=True)


# -----------------------------------------------------------------------------
# BigQuery publish validation
# -----------------------------------------------------------------------------

st.subheader("BigQuery Publish Validation")

if filtered_publish_summary_df.empty:
    st.warning("No BigQuery publish summary rows returned or matched the selected status filter.")
else:
    publish_summary_display_df = filtered_publish_summary_df.copy()

    publish_summary_display_df["source_vs_written_delta"] = (
        publish_summary_display_df["written_rows"]
        - publish_summary_display_df["source_rows"]
    )

    publish_summary_display_df["expected_vs_written_delta"] = (
        publish_summary_display_df["written_rows"]
        - publish_summary_display_df["expected_rows"]
    )

    publish_col1, publish_col2 = st.columns([1.2, 1])

    with publish_col1:
        publish_display_columns = [
            "target_table",
            "description",
            "expected_rows",
            "source_rows",
            "written_rows",
            "source_vs_written_delta",
            "expected_vs_written_delta",
            "status",
            "published_at_utc",
        ]

        available_publish_display_columns = [
            column_name
            for column_name in publish_display_columns
            if column_name in publish_summary_display_df.columns
        ]

        st.dataframe(
            publish_summary_display_df[available_publish_display_columns],
            use_container_width=True,
            hide_index=True,
        )

    with publish_col2:
        status_count_df = (
            publish_summary_display_df
            .groupby("status", dropna=False)
            .size()
            .reset_index(name="target_count")
            .sort_values("target_count", ascending=False)
        )

        fig_publish_status = px.bar(
            status_count_df,
            x="status",
            y="target_count",
            text="target_count",
            title="BigQuery Publish Status Counts",
        )

        st.plotly_chart(fig_publish_status, use_container_width=True)


# -----------------------------------------------------------------------------
# Pipeline health context
# -----------------------------------------------------------------------------

st.subheader("Pipeline Health Context")

if pipeline_health_df.empty:
    st.warning("No pipeline health rows returned from BigQuery.")
else:
    pipeline_context_columns = [
        "pipeline_name",
        "latest_status",
        "latest_run_started_at",
        "latest_run_completed_at",
        "latest_rows_read",
        "latest_rows_accepted",
        "latest_rows_rejected",
        "run_count",
        "failed_count",
        "partial_load_count",
        "failure_rate",
        "overall_rejected_rate",
    ]

    available_pipeline_context_columns = [
        column_name
        for column_name in pipeline_context_columns
        if column_name in pipeline_health_df.columns
    ]

    st.dataframe(
        pipeline_health_df[available_pipeline_context_columns],
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------------------------------------------------------
# Selected checkpoint drilldown
# -----------------------------------------------------------------------------

st.subheader("Selected Checkpoint Drilldown")

drilldown_df = filtered_reconciliation_df.copy()

drilldown_df["checkpoint_label"] = (
    drilldown_df["stage_order"].astype("Int64").astype(str)
    + " | "
    + drilldown_df["layer"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["object_name"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["metric_type"].fillna("UNKNOWN").astype(str)
)

checkpoint_options = (
    drilldown_df[["stage_order", "checkpoint_label"]]
    .drop_duplicates()
    .sort_values("stage_order")
)

checkpoint_label_by_stage = dict(
    zip(
        checkpoint_options["stage_order"],
        checkpoint_options["checkpoint_label"],
    )
)

selected_stage_order = st.selectbox(
    "Select a reconciliation checkpoint to inspect",
    options=checkpoint_options["stage_order"].tolist(),
    format_func=lambda value: checkpoint_label_by_stage.get(value, str(value)),
)

selected_checkpoint_rows = drilldown_df[
    drilldown_df["stage_order"] == selected_stage_order
]

if selected_checkpoint_rows.empty:
    st.warning("Selected checkpoint was not found.")
else:
    selected_checkpoint = selected_checkpoint_rows.iloc[0]

    detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)

    with detail_col1:
        st.metric("Layer", str(selected_checkpoint.get("layer", "N/A")))

    with detail_col2:
        st.metric("Object", str(selected_checkpoint.get("object_name", "N/A")))

    with detail_col3:
        st.metric("Row Count", format_int(selected_checkpoint.get("row_count")))

    with detail_col4:
        delta_value = selected_checkpoint.get("row_count_delta_from_previous")
        st.metric("Delta from Previous", format_int(delta_value))

    st.markdown("#### Checkpoint Notes")
    st.info(str(selected_checkpoint.get("notes", "N/A")))

    st.markdown("#### Interpretation")
    st.success(str(selected_checkpoint.get("interpretation", "N/A")))

    checkpoint_context = {
        "stage_order": int(selected_checkpoint.get("stage_order")),
        "layer": selected_checkpoint.get("layer"),
        "object_name": selected_checkpoint.get("object_name"),
        "row_count": None
        if pd.isna(selected_checkpoint.get("row_count"))
        else int(selected_checkpoint.get("row_count")),
        "previous_row_count": None
        if pd.isna(selected_checkpoint.get("previous_row_count"))
        else int(selected_checkpoint.get("previous_row_count")),
        "row_count_delta_from_previous": None
        if pd.isna(selected_checkpoint.get("row_count_delta_from_previous"))
        else int(selected_checkpoint.get("row_count_delta_from_previous")),
        "row_count_delta_pct_from_previous": None
        if pd.isna(selected_checkpoint.get("row_count_delta_pct_from_previous"))
        else float(selected_checkpoint.get("row_count_delta_pct_from_previous")),
        "metric_type": selected_checkpoint.get("metric_type"),
        "notes": selected_checkpoint.get("notes"),
        "interpretation": selected_checkpoint.get("interpretation"),
        "bigquery_publish_run_id": selected_checkpoint.get("_bigquery_publish_run_id"),
        "bigquery_published_at_utc": str(
            selected_checkpoint.get("_bigquery_published_at_utc")
        ),
    }

    st.markdown("#### Checkpoint JSON Context")
    st.json(checkpoint_context)


# -----------------------------------------------------------------------------
# Download and metadata
# -----------------------------------------------------------------------------

st.subheader("Export")

download_col1, download_col2 = st.columns(2)

with download_col1:
    reconciliation_csv = (
        filtered_reconciliation_df[available_reconciliation_display_columns]
        .to_csv(index=False)
        .encode("utf-8")
    )

    st.download_button(
        label="Download reconciliation checkpoints as CSV",
        data=reconciliation_csv,
        file_name="meterflow_iq_reconciliation_checkpoints.csv",
        mime="text/csv",
    )

with download_col2:
    if not filtered_publish_summary_df.empty:
        publish_csv = (
            publish_summary_display_df
            .to_csv(index=False)
            .encode("utf-8")
        )

        st.download_button(
            label="Download BigQuery publish summary as CSV",
            data=publish_csv,
            file_name="meterflow_iq_bigquery_publish_summary.csv",
            mime="text/csv",
        )


with st.expander("BigQuery publish metadata"):
    metadata_columns = [
        "_bigquery_publish_run_id",
        "_bigquery_published_at_utc",
    ]

    available_metadata_columns = [
        column_name
        for column_name in metadata_columns
        if column_name in reconciliation_df.columns
    ]

    if available_metadata_columns:
        st.write("Reconciliation publish metadata")
        st.dataframe(
            reconciliation_df[available_metadata_columns].drop_duplicates(),
            use_container_width=True,
            hide_index=True,
        )

    if not publish_summary_df.empty:
        publish_metadata_columns = [
            "publish_run_id",
            "published_at_utc",
        ]

        available_publish_metadata_columns = [
            column_name
            for column_name in publish_metadata_columns
            if column_name in publish_summary_df.columns
        ]

        if available_publish_metadata_columns:
            st.write("BigQuery publish summary metadata")
            st.dataframe(
                publish_summary_df[available_publish_metadata_columns].drop_duplicates(),
                use_container_width=True,
                hide_index=True,
            )
