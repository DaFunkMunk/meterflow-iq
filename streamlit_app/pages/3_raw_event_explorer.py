"""
MeterFlow IQ - Raw Event Explorer Page

Shows MongoDB-derived event/source context that was carried through
Databricks Silver/Gold and published to BigQuery.

Uses:
- fact_data_quality_exception
- vw_streamlit_rca_context

Important MVP note:
This page does not display the full original MongoDB raw JSON document.
It displays the MongoDB-derived event fields that were flattened/enriched
into the Gold exception fact and published to BigQuery.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List

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
    page_title="MeterFlow IQ - Raw Event Explorer",
    page_icon="🧾",
    layout="wide",
)

st.title("Raw Event Explorer")
st.caption(
    "Inspect MongoDB-derived device, communication, source-system, polling-platform, "
    "and scenario context behind data-quality exceptions."
)

st.info(get_environment_label())

st.warning(
    "MVP note: this page shows MongoDB-derived fields published into BigQuery. "
    "The full original MongoDB raw JSON payload is preserved upstream in the "
    "Databricks/MongoDB path, but it is not exposed in this current BigQuery view."
)


with st.sidebar:
    st.header("Raw Event Filters")

    if st.button("Refresh BigQuery cache"):
        clear_streamlit_caches()
        st.rerun()

    st.caption(get_bigquery_auth_caption())


# -----------------------------------------------------------------------------
# Load BigQuery data
# -----------------------------------------------------------------------------

raw_event_sql = f"""
SELECT
  exception_id,
  rule_id,
  exception_type,
  severity,
  business_reason,
  raw_reading_id,
  meter_id,
  source_facility_id,
  facility_id,
  facility_name,
  region,
  basin,
  reading_timestamp,
  production_date,
  poll_timestamp,
  load_timestamp,
  source_system,
  polling_platform,
  batch_id,
  volume,
  quality_code,
  raw_status,
  mongo_event_id,
  mongo_device_status,
  mongo_communication_status,
  mongo_battery_status,
  mongo_signal_quality,
  mongo_scenario_id,
  primary_exception_type,
  detected_at,
  _bigquery_publish_run_id,
  _bigquery_published_at_utc
FROM {fq_table("fact_data_quality_exception")}
ORDER BY production_date DESC, facility_id, meter_id, exception_type
"""

rca_context_sql = f"""
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
FROM {fq_table("vw_streamlit_rca_context")}
ORDER BY production_date DESC, exception_count DESC
"""

try:
    raw_event_df = run_query(raw_event_sql)
    rca_context_df = run_query(rca_context_sql)
except Exception as exc:
    st.error("BigQuery query failed.")
    st.exception(exc)
    st.stop()


if raw_event_df.empty:
    st.warning("No raw event context rows returned from BigQuery.")
    st.stop()


# -----------------------------------------------------------------------------
# Normalize data types
# -----------------------------------------------------------------------------

datetime_columns = [
    "reading_timestamp",
    "production_date",
    "poll_timestamp",
    "load_timestamp",
    "detected_at",
    "_bigquery_published_at_utc",
]

for column_name in datetime_columns:
    if column_name in raw_event_df.columns:
        raw_event_df[column_name] = pd.to_datetime(
            raw_event_df[column_name],
            errors="coerce",
        )

numeric_columns = [
    "volume",
]

for column_name in numeric_columns:
    if column_name in raw_event_df.columns:
        raw_event_df[column_name] = pd.to_numeric(
            raw_event_df[column_name],
            errors="coerce",
        )

if not rca_context_df.empty:
    rca_datetime_columns = [
        "production_date",
        "_bigquery_published_at_utc",
    ]

    for column_name in rca_datetime_columns:
        if column_name in rca_context_df.columns:
            rca_context_df[column_name] = pd.to_datetime(
                rca_context_df[column_name],
                errors="coerce",
            )

    rca_numeric_columns = [
        "exception_count",
        "affected_reading_count",
        "affected_meter_count",
    ]

    for column_name in rca_numeric_columns:
        if column_name in rca_context_df.columns:
            rca_context_df[column_name] = pd.to_numeric(
                rca_context_df[column_name],
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
    Apply a multiselect filter to a dataframe column.

    Empty selection means include all.
    """
    if not selected_values or column_name not in df.columns:
        return df

    return df[df[column_name].astype(str).isin(selected_values)]


def safe_json_value(value: Any) -> Any:
    """
    Convert pandas/numpy scalar values into Streamlit JSON-safe values.
    """
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)

    return value


# -----------------------------------------------------------------------------
# Sidebar filters
# -----------------------------------------------------------------------------

valid_dates = raw_event_df["production_date"].dropna()

if valid_dates.empty:
    st.warning("Raw event context does not contain valid production dates.")
    st.stop()

min_date = valid_dates.min().date()
max_date = valid_dates.max().date()

selected_date_range = st.sidebar.date_input(
    "Production date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(selected_date_range, tuple):
    if len(selected_date_range) == 2:
        start_date = selected_date_range[0]
        end_date = selected_date_range[1]
    elif len(selected_date_range) == 1:
        start_date = selected_date_range[0]
        end_date = selected_date_range[0]
    else:
        start_date = min_date
        end_date = max_date
else:
    start_date = selected_date_range
    end_date = selected_date_range

if start_date is None or end_date is None:
    st.sidebar.warning("Select both a start and end date.")
    st.stop()

if start_date > end_date:
    st.error("Start date cannot be after end date.")
    st.stop()


source_system_options = get_string_options(raw_event_df, "source_system")
polling_platform_options = get_string_options(raw_event_df, "polling_platform")
device_status_options = get_string_options(raw_event_df, "mongo_device_status")
communication_status_options = get_string_options(raw_event_df, "mongo_communication_status")
battery_status_options = get_string_options(raw_event_df, "mongo_battery_status")
signal_quality_options = get_string_options(raw_event_df, "mongo_signal_quality")
exception_type_options = get_string_options(raw_event_df, "exception_type")
scenario_options = get_string_options(raw_event_df, "mongo_scenario_id")
facility_options = get_string_options(raw_event_df, "facility_id")

selected_source_systems = st.sidebar.multiselect(
    "Source system",
    options=source_system_options,
    default=source_system_options,
)

selected_polling_platforms = st.sidebar.multiselect(
    "Polling platform",
    options=polling_platform_options,
    default=polling_platform_options,
)

selected_device_statuses = st.sidebar.multiselect(
    "Device status",
    options=device_status_options,
    default=device_status_options,
)

selected_communication_statuses = st.sidebar.multiselect(
    "Communication status",
    options=communication_status_options,
    default=communication_status_options,
)

selected_battery_statuses = st.sidebar.multiselect(
    "Battery status",
    options=battery_status_options,
    default=battery_status_options,
)

selected_signal_qualities = st.sidebar.multiselect(
    "Signal quality",
    options=signal_quality_options,
    default=signal_quality_options,
)

selected_exception_types = st.sidebar.multiselect(
    "Exception type",
    options=exception_type_options,
    default=exception_type_options,
)

selected_scenarios = st.sidebar.multiselect(
    "Scenario ID",
    options=scenario_options,
    default=[],
    help="Leave blank to include all scenarios.",
)

selected_facilities = st.sidebar.multiselect(
    "Facility",
    options=facility_options,
    default=[],
    help="Leave blank to include all facilities.",
)

meter_search = st.sidebar.text_input(
    "Meter search",
    value="",
    help="Optional partial meter_id search.",
).strip()

raw_reading_search = st.sidebar.text_input(
    "Raw reading search",
    value="",
    help="Optional partial raw_reading_id search.",
).strip()


# -----------------------------------------------------------------------------
# Apply filters
# -----------------------------------------------------------------------------

filtered_df = raw_event_df.copy()

filtered_df = filtered_df[
    (filtered_df["production_date"].dt.date >= start_date)
    & (filtered_df["production_date"].dt.date <= end_date)
]

filtered_df = apply_multiselect_filter(
    filtered_df,
    "source_system",
    selected_source_systems,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "polling_platform",
    selected_polling_platforms,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "mongo_device_status",
    selected_device_statuses,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "mongo_communication_status",
    selected_communication_statuses,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "mongo_battery_status",
    selected_battery_statuses,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "mongo_signal_quality",
    selected_signal_qualities,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "exception_type",
    selected_exception_types,
)

if selected_scenarios:
    filtered_df = apply_multiselect_filter(
        filtered_df,
        "mongo_scenario_id",
        selected_scenarios,
    )

if selected_facilities:
    filtered_df = apply_multiselect_filter(
        filtered_df,
        "facility_id",
        selected_facilities,
    )

if meter_search:
    filtered_df = filtered_df[
        filtered_df["meter_id"]
        .fillna("")
        .astype(str)
        .str.contains(meter_search, case=False, na=False)
    ]

if raw_reading_search:
    filtered_df = filtered_df[
        filtered_df["raw_reading_id"]
        .fillna("")
        .astype(str)
        .str.contains(raw_reading_search, case=False, na=False)
    ]

if filtered_df.empty:
    st.warning("No raw event context rows match the selected filters.")
    st.stop()


# -----------------------------------------------------------------------------
# KPI cards
# -----------------------------------------------------------------------------

event_context_rows = len(filtered_df)
unique_raw_readings = filtered_df["raw_reading_id"].nunique()
unique_meters = filtered_df["meter_id"].nunique()
unique_facilities = filtered_df["facility_id"].nunique()

device_issue_count = int(
    filtered_df["mongo_device_status"]
    .fillna("")
    .astype(str)
    .str.upper()
    .isin(["NO_SIGNAL", "OFFLINE", "FAILED"])
    .sum()
)

communication_issue_count = int(
    filtered_df["mongo_communication_status"]
    .fillna("")
    .astype(str)
    .str.upper()
    .isin(["FAILED", "DEGRADED"])
    .sum()
)

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.metric("Event Context Rows", f"{event_context_rows:,}")

with kpi2:
    st.metric("Raw Readings", f"{unique_raw_readings:,}")

with kpi3:
    st.metric("Meters", f"{unique_meters:,}")

with kpi4:
    st.metric("Facilities", f"{unique_facilities:,}")

with kpi5:
    st.metric("Device / Comms Issues", f"{device_issue_count + communication_issue_count:,}")


# -----------------------------------------------------------------------------
# Charts
# -----------------------------------------------------------------------------

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Device Status Distribution")

    device_status_counts = (
        filtered_df
        .groupby("mongo_device_status", dropna=False)
        .size()
        .reset_index(name="event_rows")
        .sort_values("event_rows", ascending=False)
    )

    fig_device_status = px.bar(
        device_status_counts,
        x="mongo_device_status",
        y="event_rows",
        text="event_rows",
        title="Rows by MongoDB-Derived Device Status",
    )

    st.plotly_chart(fig_device_status, use_container_width=True)

with chart_col2:
    st.subheader("Communication Status Distribution")

    communication_status_counts = (
        filtered_df
        .groupby("mongo_communication_status", dropna=False)
        .size()
        .reset_index(name="event_rows")
        .sort_values("event_rows", ascending=False)
    )

    fig_communication_status = px.bar(
        communication_status_counts,
        x="mongo_communication_status",
        y="event_rows",
        text="event_rows",
        title="Rows by MongoDB-Derived Communication Status",
    )

    st.plotly_chart(fig_communication_status, use_container_width=True)


chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    st.subheader("Signal Quality Distribution")

    signal_quality_counts = (
        filtered_df
        .groupby("mongo_signal_quality", dropna=False)
        .size()
        .reset_index(name="event_rows")
        .sort_values("event_rows", ascending=False)
    )

    fig_signal_quality = px.bar(
        signal_quality_counts,
        x="mongo_signal_quality",
        y="event_rows",
        text="event_rows",
        title="Rows by MongoDB-Derived Signal Quality",
    )

    st.plotly_chart(fig_signal_quality, use_container_width=True)

with chart_col4:
    st.subheader("Polling Platform Distribution")

    platform_counts = (
        filtered_df
        .groupby("polling_platform", dropna=False)
        .size()
        .reset_index(name="event_rows")
        .sort_values("event_rows", ascending=False)
    )

    fig_platform = px.bar(
        platform_counts,
        x="polling_platform",
        y="event_rows",
        text="event_rows",
        title="Rows by Polling Platform",
    )

    st.plotly_chart(fig_platform, use_container_width=True)


st.subheader("Event Rows by Production Date")

daily_event_counts = (
    filtered_df
    .groupby("production_date", dropna=False)
    .size()
    .reset_index(name="event_rows")
    .sort_values("production_date")
)

fig_daily_events = px.line(
    daily_event_counts,
    x="production_date",
    y="event_rows",
    markers=True,
    title="Raw Event Context Rows by Production Date",
)

st.plotly_chart(fig_daily_events, use_container_width=True)


# -----------------------------------------------------------------------------
# Raw event context detail table
# -----------------------------------------------------------------------------

st.subheader("Raw Event Context Detail")

detail_display_columns = [
    "mongo_event_id",
    "raw_reading_id",
    "exception_id",
    "exception_type",
    "severity",
    "meter_id",
    "facility_id",
    "facility_name",
    "region",
    "basin",
    "production_date",
    "reading_timestamp",
    "poll_timestamp",
    "load_timestamp",
    "source_system",
    "polling_platform",
    "batch_id",
    "volume",
    "quality_code",
    "raw_status",
    "mongo_device_status",
    "mongo_communication_status",
    "mongo_battery_status",
    "mongo_signal_quality",
    "mongo_scenario_id",
    "primary_exception_type",
]

available_detail_columns = [
    column_name
    for column_name in detail_display_columns
    if column_name in filtered_df.columns
]

st.dataframe(
    filtered_df[available_detail_columns],
    use_container_width=True,
    hide_index=True,
)

csv_bytes = filtered_df[available_detail_columns].to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download filtered raw event context as CSV",
    data=csv_bytes,
    file_name="meterflow_iq_raw_event_context.csv",
    mime="text/csv",
)


# -----------------------------------------------------------------------------
# Selected event drilldown
# -----------------------------------------------------------------------------

st.subheader("Selected Raw Event / Exception Drilldown")

drilldown_df = filtered_df.copy()

drilldown_df["event_label"] = (
    drilldown_df["production_date"].dt.date.astype(str)
    + " | "
    + drilldown_df["facility_id"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["meter_id"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["exception_type"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["raw_reading_id"].fillna("UNKNOWN").astype(str)
)

drilldown_options = (
    drilldown_df[["exception_id", "event_label"]]
    .drop_duplicates()
    .head(1000)
)

event_label_by_exception_id = dict(
    zip(
        drilldown_options["exception_id"],
        drilldown_options["event_label"],
    )
)

selected_exception_id = st.selectbox(
    "Select a raw event context row to inspect",
    options=drilldown_options["exception_id"].tolist(),
    format_func=lambda value: event_label_by_exception_id.get(value, value),
)

selected_rows = filtered_df[
    filtered_df["exception_id"] == selected_exception_id
]

if selected_rows.empty:
    st.warning("Selected event context row was not found in the filtered data.")
else:
    selected_row = selected_rows.iloc[0]

    detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)

    with detail_col1:
        st.metric("Exception Type", str(selected_row.get("exception_type", "N/A")))
        st.metric("Severity", str(selected_row.get("severity", "N/A")))

    with detail_col2:
        st.metric("Device Status", str(selected_row.get("mongo_device_status", "N/A")))
        st.metric("Communication", str(selected_row.get("mongo_communication_status", "N/A")))

    with detail_col3:
        st.metric("Signal Quality", str(selected_row.get("mongo_signal_quality", "N/A")))
        st.metric("Battery Status", str(selected_row.get("mongo_battery_status", "N/A")))

    with detail_col4:
        st.metric("Source System", str(selected_row.get("source_system", "N/A")))
        st.metric("Polling Platform", str(selected_row.get("polling_platform", "N/A")))

    st.markdown("#### Business Reason")
    st.info(str(selected_row.get("business_reason", "N/A")))

    st.markdown("#### Reconstructed Event Context")

    event_context = {
        "mongo_event_id": safe_json_value(selected_row.get("mongo_event_id")),
        "raw_reading_id": safe_json_value(selected_row.get("raw_reading_id")),
        "exception_id": safe_json_value(selected_row.get("exception_id")),
        "rule_id": safe_json_value(selected_row.get("rule_id")),
        "exception_type": safe_json_value(selected_row.get("exception_type")),
        "severity": safe_json_value(selected_row.get("severity")),
        "meter_id": safe_json_value(selected_row.get("meter_id")),
        "source_facility_id": safe_json_value(selected_row.get("source_facility_id")),
        "facility_id": safe_json_value(selected_row.get("facility_id")),
        "facility_name": safe_json_value(selected_row.get("facility_name")),
        "region": safe_json_value(selected_row.get("region")),
        "basin": safe_json_value(selected_row.get("basin")),
        "production_date": safe_json_value(selected_row.get("production_date")),
        "reading_timestamp": safe_json_value(selected_row.get("reading_timestamp")),
        "poll_timestamp": safe_json_value(selected_row.get("poll_timestamp")),
        "load_timestamp": safe_json_value(selected_row.get("load_timestamp")),
        "source_system": safe_json_value(selected_row.get("source_system")),
        "polling_platform": safe_json_value(selected_row.get("polling_platform")),
        "batch_id": safe_json_value(selected_row.get("batch_id")),
        "volume": safe_json_value(selected_row.get("volume")),
        "quality_code": safe_json_value(selected_row.get("quality_code")),
        "raw_status": safe_json_value(selected_row.get("raw_status")),
        "mongo_device_status": safe_json_value(selected_row.get("mongo_device_status")),
        "mongo_communication_status": safe_json_value(selected_row.get("mongo_communication_status")),
        "mongo_battery_status": safe_json_value(selected_row.get("mongo_battery_status")),
        "mongo_signal_quality": safe_json_value(selected_row.get("mongo_signal_quality")),
        "mongo_scenario_id": safe_json_value(selected_row.get("mongo_scenario_id")),
        "primary_exception_type": safe_json_value(selected_row.get("primary_exception_type")),
        "detected_at": safe_json_value(selected_row.get("detected_at")),
    }

    st.json(event_context)

    selected_scenario_id = safe_json_value(selected_row.get("mongo_scenario_id"))
    selected_facility_id = safe_json_value(selected_row.get("facility_id"))
    selected_exception_type = safe_json_value(selected_row.get("exception_type"))

    if not rca_context_df.empty:
        related_rca_df = rca_context_df.copy()

        if selected_scenario_id:
            related_rca_df = related_rca_df[
                related_rca_df["scenario_id"]
                .fillna("")
                .astype(str)
                == str(selected_scenario_id)
            ]

        if related_rca_df.empty and selected_facility_id and selected_exception_type:
            related_rca_df = rca_context_df[
                (rca_context_df["facility_id"].fillna("").astype(str) == str(selected_facility_id))
                & (rca_context_df["exception_type"].fillna("").astype(str) == str(selected_exception_type))
            ]

        st.markdown("#### Related RCA / Known Scenario Context")

        if related_rca_df.empty:
            st.warning(
                "No related RCA context found for this event's scenario/facility/exception combination."
            )
        else:
            rca_display_columns = [
                "production_date",
                "facility_id",
                "facility_name",
                "exception_type",
                "severity",
                "exception_count",
                "affected_reading_count",
                "affected_meter_count",
                "scenario_id",
                "scenario_name",
                "expected_root_cause",
                "recommended_next_step",
                "rca_summary_seed",
            ]

            available_rca_display_columns = [
                column_name
                for column_name in rca_display_columns
                if column_name in related_rca_df.columns
            ]

            st.dataframe(
                related_rca_df[available_rca_display_columns].head(10),
                use_container_width=True,
                hide_index=True,
            )

            first_rca_row = related_rca_df.iloc[0]

            if "recommended_next_step" in related_rca_df.columns:
                recommended_next_step = first_rca_row.get("recommended_next_step")
                if recommended_next_step is not None and str(recommended_next_step).strip():
                    st.markdown("#### Recommended Next Step")
                    st.success(str(recommended_next_step))

            if "rca_summary_seed" in related_rca_df.columns:
                rca_summary_seed = first_rca_row.get("rca_summary_seed")
                if rca_summary_seed is not None and str(rca_summary_seed).strip():
                    st.markdown("#### RCA Summary Seed")
                    st.info(str(rca_summary_seed))


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
        st.write("Raw event context publish metadata")
        st.dataframe(
            filtered_df[available_metadata_columns].drop_duplicates(),
            use_container_width=True,
            hide_index=True,
        )

    if not rca_context_df.empty:
        available_rca_metadata_columns = [
            column_name
            for column_name in metadata_columns
            if column_name in rca_context_df.columns
        ]

        if available_rca_metadata_columns:
            st.write("RCA context publish metadata")
            st.dataframe(
                rca_context_df[available_rca_metadata_columns].drop_duplicates(),
                use_container_width=True,
                hide_index=True,
            )
