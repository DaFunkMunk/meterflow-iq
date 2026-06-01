"""
MeterFlow IQ - Exceptions Page

Shows data-quality exception triage from BigQuery.

Uses:
- vw_streamlit_exception_detail
- vw_streamlit_exception_summary

Purpose:
Help a support analyst filter, inspect, and explain data-quality exceptions
by facility, meter, date, rule, severity, source system, and polling platform.
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
    get_bigquery_auth_caption,
    get_environment_label,
    run_query,
)


st.set_page_config(
    page_title="MeterFlow IQ - Exceptions",
    page_icon="🚨",
    layout="wide",
)

st.title("Data Quality Exceptions")
st.caption(
    "Filter, triage, and inspect data-quality exceptions by facility, meter, date, "
    "rule, severity, source system, and polling platform."
)

st.info(get_environment_label())


with st.sidebar:
    st.header("Exception Filters")

    if st.button("Refresh BigQuery cache"):
        clear_streamlit_caches()
        st.rerun()

    st.caption(get_bigquery_auth_caption())


# -----------------------------------------------------------------------------
# Load BigQuery data
# -----------------------------------------------------------------------------

exception_detail_sql = f"""
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
FROM {fq_table("vw_streamlit_exception_detail")}
ORDER BY production_date DESC, facility_id, meter_id, exception_type
"""

exception_summary_sql = f"""
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
FROM {fq_table("vw_streamlit_exception_summary")}
ORDER BY production_date DESC, exception_count DESC
"""

try:
    exception_detail_df = run_query(exception_detail_sql)
    exception_summary_df = run_query(exception_summary_sql)
except Exception as exc:
    st.error("BigQuery query failed.")
    st.exception(exc)
    st.stop()


if exception_detail_df.empty:
    st.warning("No exception detail rows returned from BigQuery.")
    st.stop()


# -----------------------------------------------------------------------------
# Normalize data types
# -----------------------------------------------------------------------------

date_columns_detail = [
    "production_date",
    "reading_timestamp",
    "_bigquery_published_at_utc",
]

for column_name in date_columns_detail:
    if column_name in exception_detail_df.columns:
        exception_detail_df[column_name] = pd.to_datetime(
            exception_detail_df[column_name],
            errors="coerce",
        )

numeric_columns_detail = [
    "volume",
    "days_since_production_date",
]

for column_name in numeric_columns_detail:
    if column_name in exception_detail_df.columns:
        exception_detail_df[column_name] = pd.to_numeric(
            exception_detail_df[column_name],
            errors="coerce",
        )

if not exception_summary_df.empty:
    date_columns_summary = [
        "production_date",
        "first_detected_at",
        "last_detected_at",
        "_bigquery_published_at_utc",
    ]

    for column_name in date_columns_summary:
        if column_name in exception_summary_df.columns:
            exception_summary_df[column_name] = pd.to_datetime(
                exception_summary_df[column_name],
                errors="coerce",
            )

    numeric_columns_summary = [
        "exception_rule_failure_count",
        "exception_count",
        "affected_reading_count",
        "affected_meter_count",
    ]

    for column_name in numeric_columns_summary:
        if column_name in exception_summary_df.columns:
            exception_summary_df[column_name] = pd.to_numeric(
                exception_summary_df[column_name],
                errors="coerce",
            )


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def get_string_options(df: pd.DataFrame, column_name: str) -> list:
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
    selected_values: list,
) -> pd.DataFrame:
    """
    Apply a multiselect filter to a dataframe column.
    """
    if not selected_values or column_name not in df.columns:
        return df

    return df[df[column_name].astype(str).isin(selected_values)]


def format_pct(value) -> str:
    """
    Format a percent-like numeric value safely.
    """
    if value is None or pd.isna(value):
        return "N/A"

    return f"{value:.2%}"


# -----------------------------------------------------------------------------
# Sidebar filters
# -----------------------------------------------------------------------------

valid_dates = exception_detail_df["production_date"].dropna()

if valid_dates.empty:
    st.warning("Exception data does not contain valid production dates.")
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


severity_options = get_string_options(exception_detail_df, "severity")
exception_type_options = get_string_options(exception_detail_df, "exception_type")
region_options = get_string_options(exception_detail_df, "region")
facility_options = get_string_options(exception_detail_df, "facility_id")
source_system_options = get_string_options(exception_detail_df, "source_system")
polling_platform_options = get_string_options(exception_detail_df, "polling_platform")
status_options = get_string_options(exception_detail_df, "exception_status")

selected_severities = st.sidebar.multiselect(
    "Severity",
    options=severity_options,
    default=severity_options,
)

selected_exception_types = st.sidebar.multiselect(
    "Exception type",
    options=exception_type_options,
    default=exception_type_options,
)

selected_regions = st.sidebar.multiselect(
    "Region",
    options=region_options,
    default=region_options,
)

selected_facilities = st.sidebar.multiselect(
    "Facility",
    options=facility_options,
    default=[],
    help="Leave blank to include all facilities.",
)

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

selected_statuses = st.sidebar.multiselect(
    "Exception status",
    options=status_options,
    default=status_options,
)

meter_search = st.sidebar.text_input(
    "Meter search",
    value="",
    help="Optional partial meter_id search.",
).strip()


# -----------------------------------------------------------------------------
# Apply filters
# -----------------------------------------------------------------------------

filtered_df = exception_detail_df.copy()

filtered_df = filtered_df[
    (filtered_df["production_date"].dt.date >= start_date)
    & (filtered_df["production_date"].dt.date <= end_date)
]

filtered_df = apply_multiselect_filter(
    filtered_df,
    "severity",
    selected_severities,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "exception_type",
    selected_exception_types,
)

filtered_df = apply_multiselect_filter(
    filtered_df,
    "region",
    selected_regions,
)

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
    "exception_status",
    selected_statuses,
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

if filtered_df.empty:
    st.warning("No exception rows match the selected filters.")
    st.stop()


# -----------------------------------------------------------------------------
# KPI cards
# -----------------------------------------------------------------------------

rule_failure_count = len(filtered_df)
affected_reading_count = filtered_df["raw_reading_id"].nunique()
affected_meter_count = filtered_df["meter_id"].nunique()
affected_facility_count = filtered_df["facility_id"].nunique()

high_severity_count = int(
    filtered_df["severity"]
    .fillna("")
    .astype(str)
    .str.upper()
    .str.contains("HIGH")
    .sum()
)

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.metric("Rule Failures", f"{rule_failure_count:,}")

with kpi2:
    st.metric("Affected Readings", f"{affected_reading_count:,}")

with kpi3:
    st.metric("Affected Meters", f"{affected_meter_count:,}")

with kpi4:
    st.metric("Affected Facilities", f"{affected_facility_count:,}")

with kpi5:
    st.metric("High-Severity Failures", f"{high_severity_count:,}")


# -----------------------------------------------------------------------------
# Charts
# -----------------------------------------------------------------------------

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Top Exception Types")

    exception_type_counts = (
        filtered_df
        .groupby("exception_type", dropna=False)
        .size()
        .reset_index(name="exception_rows")
        .sort_values("exception_rows", ascending=False)
        .head(10)
    )

    fig_exception_type = px.bar(
        exception_type_counts,
        x="exception_type",
        y="exception_rows",
        text="exception_rows",
        title="Top Exception Types by Rule-Failure Rows",
    )

    st.plotly_chart(fig_exception_type, use_container_width=True)

with chart_col2:
    st.subheader("Top Affected Facilities")

    facility_counts = (
        filtered_df
        .groupby(["facility_id", "facility_name"], dropna=False)
        .size()
        .reset_index(name="exception_rows")
        .sort_values("exception_rows", ascending=False)
        .head(10)
    )

    fig_facility = px.bar(
        facility_counts,
        x="facility_id",
        y="exception_rows",
        text="exception_rows",
        hover_data=["facility_name"],
        title="Top Facilities by Exception Rows",
    )

    st.plotly_chart(fig_facility, use_container_width=True)


chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    st.subheader("Severity Distribution")

    severity_counts = (
        filtered_df
        .groupby("severity", dropna=False)
        .size()
        .reset_index(name="exception_rows")
        .sort_values("exception_rows", ascending=False)
    )

    fig_severity = px.bar(
        severity_counts,
        x="severity",
        y="exception_rows",
        text="exception_rows",
        title="Exception Rows by Severity",
    )

    st.plotly_chart(fig_severity, use_container_width=True)

with chart_col4:
    st.subheader("Source System Distribution")

    source_counts = (
        filtered_df
        .groupby("source_system", dropna=False)
        .size()
        .reset_index(name="exception_rows")
        .sort_values("exception_rows", ascending=False)
    )

    fig_source = px.bar(
        source_counts,
        x="source_system",
        y="exception_rows",
        text="exception_rows",
        title="Exception Rows by Source System",
    )

    st.plotly_chart(fig_source, use_container_width=True)


# -----------------------------------------------------------------------------
# Filtered summary from detail
# -----------------------------------------------------------------------------

st.subheader("Filtered Exception Summary")

filtered_summary = (
    filtered_df
    .groupby(
        [
            "production_date",
            "facility_id",
            "facility_name",
            "region",
            "basin",
            "exception_type",
            "severity",
        ],
        dropna=False,
    )
    .agg(
        exception_rows=("exception_id", "count"),
        affected_readings=("raw_reading_id", "nunique"),
        affected_meters=("meter_id", "nunique"),
    )
    .reset_index()
    .sort_values(
        ["exception_rows", "affected_readings"],
        ascending=False,
    )
)

summary_display_columns = [
    "production_date",
    "facility_id",
    "facility_name",
    "region",
    "basin",
    "exception_type",
    "severity",
    "exception_rows",
    "affected_readings",
    "affected_meters",
]

st.dataframe(
    filtered_summary[summary_display_columns],
    use_container_width=True,
    hide_index=True,
)


# -----------------------------------------------------------------------------
# Exception detail table
# -----------------------------------------------------------------------------

st.subheader("Exception Detail")

detail_display_columns = [
    "exception_id",
    "rule_id",
    "exception_type",
    "severity",
    "business_reason",
    "raw_reading_id",
    "meter_id",
    "facility_id",
    "facility_name",
    "region",
    "basin",
    "production_date",
    "reading_timestamp",
    "source_system",
    "polling_platform",
    "volume",
    "quality_code",
    "raw_status",
    "mongo_device_status",
    "mongo_communication_status",
    "mongo_signal_quality",
    "mongo_scenario_id",
    "primary_exception_type",
    "exception_status",
    "days_since_production_date",
]

available_detail_display_columns = [
    column_name
    for column_name in detail_display_columns
    if column_name in filtered_df.columns
]

st.dataframe(
    filtered_df[available_detail_display_columns],
    use_container_width=True,
    hide_index=True,
)

csv_bytes = filtered_df[available_detail_display_columns].to_csv(index=False).encode("utf-8")

st.download_button(
    label="Download filtered exceptions as CSV",
    data=csv_bytes,
    file_name="meterflow_iq_filtered_exceptions.csv",
    mime="text/csv",
)


# -----------------------------------------------------------------------------
# Selected exception drilldown
# -----------------------------------------------------------------------------

st.subheader("Selected Exception Drilldown")

drilldown_df = filtered_df.copy()

drilldown_df["exception_label"] = (
    drilldown_df["production_date"].dt.date.astype(str)
    + " | "
    + drilldown_df["facility_id"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["meter_id"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["exception_type"].fillna("UNKNOWN").astype(str)
    + " | "
    + drilldown_df["exception_id"].astype(str).str.slice(0, 12)
)

drilldown_options = (
    drilldown_df[["exception_id", "exception_label"]]
    .drop_duplicates()
    .head(1000)
)

exception_label_by_id = dict(
    zip(
        drilldown_options["exception_id"],
        drilldown_options["exception_label"],
    )
)

selected_exception_id = st.selectbox(
    "Select an exception to inspect",
    options=drilldown_options["exception_id"].tolist(),
    format_func=lambda value: exception_label_by_id.get(value, value),
)

selected_rows = filtered_df[
    filtered_df["exception_id"] == selected_exception_id
]

if selected_rows.empty:
    st.warning("Selected exception was not found in the filtered data.")
else:
    selected_row = selected_rows.iloc[0]

    detail_col1, detail_col2, detail_col3 = st.columns(3)

    with detail_col1:
        st.metric("Exception Type", str(selected_row.get("exception_type", "N/A")))
        st.metric("Severity", str(selected_row.get("severity", "N/A")))
        st.metric("Status", str(selected_row.get("exception_status", "N/A")))

    with detail_col2:
        st.metric("Facility", str(selected_row.get("facility_id", "N/A")))
        st.metric("Meter", str(selected_row.get("meter_id", "N/A")))
        st.metric("Source", str(selected_row.get("source_system", "N/A")))

    with detail_col3:
        st.metric("Quality Code", str(selected_row.get("quality_code", "N/A")))
        st.metric("Raw Status", str(selected_row.get("raw_status", "N/A")))
        st.metric("Device Status", str(selected_row.get("mongo_device_status", "N/A")))

    st.markdown("#### Business Reason")
    st.info(str(selected_row.get("business_reason", "N/A")))

    st.markdown("#### Investigation Context")

    context_data = {
        "exception_id": selected_row.get("exception_id"),
        "rule_id": selected_row.get("rule_id"),
        "raw_reading_id": selected_row.get("raw_reading_id"),
        "production_date": str(selected_row.get("production_date")),
        "reading_timestamp": str(selected_row.get("reading_timestamp")),
        "facility_id": selected_row.get("facility_id"),
        "facility_name": selected_row.get("facility_name"),
        "region": selected_row.get("region"),
        "basin": selected_row.get("basin"),
        "meter_id": selected_row.get("meter_id"),
        "polling_platform": selected_row.get("polling_platform"),
        "volume": selected_row.get("volume"),
        "quality_code": selected_row.get("quality_code"),
        "raw_status": selected_row.get("raw_status"),
        "mongo_device_status": selected_row.get("mongo_device_status"),
        "mongo_communication_status": selected_row.get("mongo_communication_status"),
        "mongo_signal_quality": selected_row.get("mongo_signal_quality"),
        "mongo_scenario_id": selected_row.get("mongo_scenario_id"),
        "primary_exception_type": selected_row.get("primary_exception_type"),
    }

    st.json(context_data)


# -----------------------------------------------------------------------------
# Published summary and metadata
# -----------------------------------------------------------------------------

with st.expander("Published BigQuery exception summary view"):
    if exception_summary_df.empty:
        st.warning("No rows returned from vw_streamlit_exception_summary.")
    else:
        published_summary_display_columns = [
            "production_date",
            "facility_id",
            "facility_name",
            "region",
            "basin",
            "exception_type",
            "severity",
            "exception_count",
            "affected_reading_count",
            "affected_meter_count",
            "source_systems",
            "polling_platforms",
            "device_statuses",
            "communication_statuses",
            "signal_qualities",
            "first_detected_at",
            "last_detected_at",
        ]

        available_published_summary_columns = [
            column_name
            for column_name in published_summary_display_columns
            if column_name in exception_summary_df.columns
        ]

        st.dataframe(
            exception_summary_df[available_published_summary_columns],
            use_container_width=True,
            hide_index=True,
        )

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
        st.write("Exception detail publish metadata")
        st.dataframe(
            filtered_df[available_metadata_columns].drop_duplicates(),
            use_container_width=True,
            hide_index=True,
        )

    if not exception_summary_df.empty:
        available_summary_metadata_columns = [
            column_name
            for column_name in metadata_columns
            if column_name in exception_summary_df.columns
        ]

        if available_summary_metadata_columns:
            st.write("Exception summary publish metadata")
            st.dataframe(
                exception_summary_df[
                    available_summary_metadata_columns
                ].drop_duplicates(),
                use_container_width=True,
                hide_index=True,
            )
