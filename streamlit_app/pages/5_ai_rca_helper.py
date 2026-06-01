"""
MeterFlow IQ - AI RCA Helper Page

Facts-only RCA helper for support analysts.

Current MVP:
- No external AI model call.
- Uses deterministic, rules-based summaries from BigQuery RCA context,
  exception detail, pipeline health, and reconciliation data.
- Produces a prompt preview that can later be sent to Gemini, Ollama, Groq,
  OpenAI, Azure OpenAI, or another approved model.

Future enhancement:
- Add AI_PROVIDER=gemini or similar once the facts-only page is stable.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
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
    page_title="MeterFlow IQ - AI RCA Helper",
    page_icon="🧠",
    layout="wide",
)

st.title("AI RCA Helper")
st.caption(
    "Facts-only root-cause explanation scaffold using BigQuery RCA context, "
    "exception details, pipeline health, and reconciliation checkpoints."
)

st.info(get_environment_label())

st.warning(
    "Current MVP mode: rules-based RCA only. No external AI model is called yet. "
    "Gemini can be added later as a provider after the facts-only workflow is stable."
)


with st.sidebar:
    st.header("RCA Helper Filters")

    if st.button("Refresh BigQuery cache"):
        clear_streamlit_caches()
        st.rerun()

    st.caption(get_bigquery_auth_caption())


# -----------------------------------------------------------------------------
# Load BigQuery data
# -----------------------------------------------------------------------------

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
  days_since_production_date,
  _bigquery_publish_run_id,
  _bigquery_published_at_utc
FROM {fq_table("vw_streamlit_exception_detail")}
ORDER BY production_date DESC, facility_id, meter_id, exception_type
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
    rca_context_df = run_query(rca_context_sql)
    exception_detail_df = run_query(exception_detail_sql)
    reconciliation_df = run_query(reconciliation_sql)
    pipeline_health_df = run_query(pipeline_health_sql)
except Exception as exc:
    st.error("BigQuery query failed.")
    st.exception(exc)
    st.stop()


if rca_context_df.empty:
    st.warning("No RCA context rows returned from BigQuery.")
    st.stop()


# -----------------------------------------------------------------------------
# Normalize data types
# -----------------------------------------------------------------------------

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

if not exception_detail_df.empty:
    exception_datetime_columns = [
        "production_date",
        "reading_timestamp",
        "_bigquery_published_at_utc",
    ]

    for column_name in exception_datetime_columns:
        if column_name in exception_detail_df.columns:
            exception_detail_df[column_name] = pd.to_datetime(
                exception_detail_df[column_name],
                errors="coerce",
            )

    exception_numeric_columns = [
        "volume",
        "days_since_production_date",
    ]

    for column_name in exception_numeric_columns:
        if column_name in exception_detail_df.columns:
            exception_detail_df[column_name] = pd.to_numeric(
                exception_detail_df[column_name],
                errors="coerce",
            )

if not reconciliation_df.empty:
    reconciliation_numeric_columns = [
        "stage_order",
        "row_count",
    ]

    for column_name in reconciliation_numeric_columns:
        if column_name in reconciliation_df.columns:
            reconciliation_df[column_name] = pd.to_numeric(
                reconciliation_df[column_name],
                errors="coerce",
            )

if not pipeline_health_df.empty:
    pipeline_numeric_columns = [
        "latest_rows_read",
        "latest_rows_accepted",
        "latest_rows_rejected",
        "run_count",
        "failed_count",
        "partial_load_count",
        "failure_rate",
        "overall_rejected_rate",
    ]

    for column_name in pipeline_numeric_columns:
        if column_name in pipeline_health_df.columns:
            pipeline_health_df[column_name] = pd.to_numeric(
                pipeline_health_df[column_name],
                errors="coerce",
            )

    pipeline_datetime_columns = [
        "latest_run_started_at",
        "latest_run_completed_at",
    ]

    for column_name in pipeline_datetime_columns:
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


def safe_value(value: Any, default: str = "N/A") -> str:
    """
    Return a safe string for display.
    """
    if value is None:
        return default

    try:
        if pd.isna(value):
            return default
    except Exception:
        pass

    value_as_string = str(value).strip()

    if not value_as_string:
        return default

    return value_as_string


def safe_int(value: Any) -> int:
    """
    Convert a numeric value to int safely.
    """
    if value is None:
        return 0

    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass

    try:
        return int(value)
    except Exception:
        return 0


def format_date(value: Any) -> str:
    """
    Format date-like values for display.
    """
    if value is None:
        return "N/A"

    try:
        if pd.isna(value):
            return "N/A"
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()

    return str(value)


def infer_likely_layer(
    exception_type: str,
    device_statuses: str,
    communication_statuses: str,
    signal_qualities: str,
    pipeline_context: pd.DataFrame,
) -> str:
    """
    Infer the most likely layer to check first using simple support rules.
    """
    exception_type_upper = safe_value(exception_type, "").upper()
    device_upper = safe_value(device_statuses, "").upper()
    communication_upper = safe_value(communication_statuses, "").upper()
    signal_upper = safe_value(signal_qualities, "").upper()

    if exception_type_upper in {
        "NO_SIGNAL",
        "STALE_READING",
        "ZERO_VOLUME",
    }:
        return "Source / polling platform"

    if "NO_SIGNAL" in device_upper or "FAILED" in communication_upper:
        return "Source / device communication"

    if "STALE" in signal_upper or "UNKNOWN" in signal_upper:
        return "Polling platform / source data freshness"

    if exception_type_upper in {
        "INVALID_METER",
        "INACTIVE_METER_REPORTING",
        "FACILITY_MISMATCH",
    }:
        return "Master/reference data"

    if exception_type_upper in {
        "DUPLICATE_READING",
        "DUPLICATE_RETRY",
    }:
        return "Ingestion / retry / duplicate handling"

    if exception_type_upper in {
        "LATE_ARRIVAL",
    }:
        return "Pipeline timing / schedule"

    if exception_type_upper in {
        "FUTURE_DATE",
    }:
        return "Source date mapping / validation rule"

    if exception_type_upper in {
        "INVALID_QUALITY_CODE",
    }:
        return "Source status mapping / business-rule validation"

    if not pipeline_context.empty:
        failed_or_partial_count = int(
            pipeline_context["latest_status"]
            .fillna("")
            .astype(str)
            .str.upper()
            .isin(["FAILED", "PARTIAL_SUCCESS"])
            .sum()
        )

        if failed_or_partial_count > 0:
            return "Pipeline / load reliability"

    return "Silver validation / data-quality rules"


def build_suggested_checks(
    exception_type: str,
    likely_layer: str,
) -> List[str]:
    """
    Return practical next checks for a support analyst.
    """
    exception_type_upper = safe_value(exception_type, "").upper()
    likely_layer_upper = safe_value(likely_layer, "").upper()

    checks: List[str] = []

    if "SOURCE" in likely_layer_upper or "POLLING" in likely_layer_upper:
        checks.extend(
            [
                "Check whether the source system produced the record at the expected timestamp.",
                "Compare event timestamp, poll timestamp, load timestamp, and report refresh timing.",
                "Review device status, communication status, signal quality, and polling platform context.",
            ]
        )

    if "MASTER" in likely_layer_upper or exception_type_upper in {
        "INVALID_METER",
        "INACTIVE_METER_REPORTING",
        "FACILITY_MISMATCH",
    }:
        checks.extend(
            [
                "Validate meter_id and facility_id against the master/reference table.",
                "Check active_flag and effective-date logic for the meter and facility.",
                "Confirm whether source facility assignment conflicts with master data.",
            ]
        )

    if "INGESTION" in likely_layer_upper or "DUPLICATE" in exception_type_upper:
        checks.extend(
            [
                "Group by meter_id, production_date, reading timestamp, and source system to confirm duplicate grain.",
                "Check retry behavior, duplicate source files, and repeated batch IDs.",
                "Confirm whether duplicate records are intentional business events or load artifacts.",
            ]
        )

    if "TIMING" in likely_layer_upper or exception_type_upper == "LATE_ARRIVAL":
        checks.extend(
            [
                "Check the last successful run, first bad run, and whether late records arrived after refresh.",
                "Compare load_timestamp to report cutoff or expected refresh time.",
                "Review pipeline job history for partial loads, retries, or delayed upstream extracts.",
            ]
        )

    if exception_type_upper == "FUTURE_DATE":
        checks.extend(
            [
                "Confirm whether production_date is mapped from the correct source field.",
                "Check time-zone and date-window logic.",
                "Reject or quarantine future-dated records until the source/date mapping is corrected.",
            ]
        )

    if exception_type_upper == "INVALID_QUALITY_CODE":
        checks.extend(
            [
                "Compare the source quality code to the accepted value list.",
                "Confirm whether the source introduced a new code that needs business approval.",
                "Update the mapping/rule reference only after business confirmation.",
            ]
        )

    if not checks:
        checks.extend(
            [
                "Compare source, Bronze, Silver, Gold, and BigQuery row counts for the affected date/facility.",
                "Review the DQ rule definition and confirm the business rule with the owning team.",
                "Use the selected exception detail and raw event context to validate the likely failure layer.",
            ]
        )

    # Deduplicate while preserving order.
    deduped_checks: List[str] = []

    for check in checks:
        if check not in deduped_checks:
            deduped_checks.append(check)

    return deduped_checks


def build_what_not_to_assume(
    exception_type: str,
    likely_layer: str,
) -> List[str]:
    """
    Return caution statements for responsible RCA.
    """
    exception_type_upper = safe_value(exception_type, "").upper()

    cautions = [
        "Do not assume the business volume is wrong until source data, timing, and rule filters are checked.",
        "Do not assume the dashboard is the root cause before validating upstream source-to-target counts.",
        "Do not change business rules without confirming the intended definition with the owning business/data team.",
    ]

    if exception_type_upper in {
        "ZERO_VOLUME",
        "NO_SIGNAL",
        "STALE_READING",
    }:
        cautions.append(
            "Do not assume a zero/stale reading is invalid until field conditions or device communication context are reviewed."
        )

    if exception_type_upper in {
        "INVALID_METER",
        "INACTIVE_METER_REPORTING",
        "FACILITY_MISMATCH",
    }:
        cautions.append(
            "Do not assume the source is wrong until master-data effective dates and mappings are confirmed."
        )

    if exception_type_upper in {
        "DUPLICATE_READING",
        "DUPLICATE_RETRY",
    }:
        cautions.append(
            "Do not delete duplicate-looking records until the intended business grain is confirmed."
        )

    return cautions


def build_plain_english_summary(
    selected_row: pd.Series,
    likely_layer: str,
) -> str:
    """
    Build a deterministic plain-English RCA summary.
    """
    production_date = format_date(selected_row.get("production_date"))
    facility_id = safe_value(selected_row.get("facility_id"))
    facility_name = safe_value(selected_row.get("facility_name"))
    exception_type = safe_value(selected_row.get("exception_type"))
    severity = safe_value(selected_row.get("severity"))
    exception_count = safe_int(selected_row.get("exception_count"))
    affected_reading_count = safe_int(selected_row.get("affected_reading_count"))
    affected_meter_count = safe_int(selected_row.get("affected_meter_count"))
    device_statuses = safe_value(selected_row.get("device_statuses_text"))
    communication_statuses = safe_value(selected_row.get("communication_statuses_text"))
    signal_qualities = safe_value(selected_row.get("signal_qualities_text"))
    expected_root_cause = safe_value(selected_row.get("expected_root_cause"), "")
    recommended_next_step = safe_value(selected_row.get("recommended_next_step"), "")

    summary = (
        f"On {production_date}, facility {facility_id} ({facility_name}) has "
        f"{exception_count:,} exception records of type {exception_type} with severity "
        f"{severity}. The affected set includes approximately {affected_reading_count:,} "
        f"readings across {affected_meter_count:,} meter(s). "
        f"Device context shows {device_statuses}; communication context shows "
        f"{communication_statuses}; signal quality context shows {signal_qualities}. "
        f"The first layer to check should be: {likely_layer}."
    )

    if expected_root_cause:
        summary += f" Known scenario/root-cause context suggests: {expected_root_cause}."

    if recommended_next_step:
        summary += f" Recommended next step: {recommended_next_step}"

    return summary


def build_prompt_preview(
    selected_row: pd.Series,
    likely_layer: str,
    suggested_checks: List[str],
    cautions: List[str],
    related_exceptions_df: pd.DataFrame,
    reconciliation_df: pd.DataFrame,
    pipeline_health_df: pd.DataFrame,
) -> str:
    """
    Build a prompt that can later be sent to Gemini or another model.
    """
    production_date = format_date(selected_row.get("production_date"))
    facility_id = safe_value(selected_row.get("facility_id"))
    facility_name = safe_value(selected_row.get("facility_name"))
    exception_type = safe_value(selected_row.get("exception_type"))
    severity = safe_value(selected_row.get("severity"))
    exception_count = safe_int(selected_row.get("exception_count"))
    affected_reading_count = safe_int(selected_row.get("affected_reading_count"))
    affected_meter_count = safe_int(selected_row.get("affected_meter_count"))

    source_systems = safe_value(selected_row.get("source_systems_text"))
    polling_platforms = safe_value(selected_row.get("polling_platforms_text"))
    device_statuses = safe_value(selected_row.get("device_statuses_text"))
    communication_statuses = safe_value(selected_row.get("communication_statuses_text"))
    signal_qualities = safe_value(selected_row.get("signal_qualities_text"))

    scenario_id = safe_value(selected_row.get("scenario_id"))
    scenario_name = safe_value(selected_row.get("scenario_name"))
    expected_root_cause = safe_value(selected_row.get("expected_root_cause"))
    recommended_next_step = safe_value(selected_row.get("recommended_next_step"))

    if not related_exceptions_df.empty:
        sample_exception_rows = related_exceptions_df.head(5)[
            [
                "exception_id",
                "rule_id",
                "raw_reading_id",
                "meter_id",
                "production_date",
                "quality_code",
                "raw_status",
                "mongo_device_status",
                "mongo_communication_status",
                "mongo_signal_quality",
            ]
        ].to_dict(orient="records")
    else:
        sample_exception_rows = []

    if not reconciliation_df.empty:
        reconciliation_rows = reconciliation_df[
            [
                "stage_order",
                "layer",
                "object_name",
                "row_count",
                "metric_type",
                "notes",
            ]
        ].to_dict(orient="records")
    else:
        reconciliation_rows = []

    if not pipeline_health_df.empty:
        pipeline_rows = pipeline_health_df[
            [
                "pipeline_name",
                "latest_status",
                "latest_rows_read",
                "latest_rows_accepted",
                "latest_rows_rejected",
                "run_count",
                "failed_count",
                "partial_load_count",
            ]
        ].to_dict(orient="records")
    else:
        pipeline_rows = []

    prompt = f"""
You are an operations data support assistant.

Use only the structured facts below.
Do not invent causes.
Do not claim a final root cause unless the facts support it.
Summarize the issue, likely data layer, recommended checks, and what not to assume yet.

Facts:
- Production date: {production_date}
- Facility: {facility_id} / {facility_name}
- Exception type: {exception_type}
- Severity: {severity}
- Exception count: {exception_count}
- Affected reading count: {affected_reading_count}
- Affected meter count: {affected_meter_count}
- Source systems: {source_systems}
- Polling platforms: {polling_platforms}
- Device statuses: {device_statuses}
- Communication statuses: {communication_statuses}
- Signal qualities: {signal_qualities}
- Scenario ID: {scenario_id}
- Scenario name: {scenario_name}
- Expected root cause from known scenario table: {expected_root_cause}
- Recommended next step from known scenario table: {recommended_next_step}
- Rules-based likely layer to check first: {likely_layer}

Suggested checks from rules engine:
{chr(10).join("- " + check for check in suggested_checks)}

What not to assume:
{chr(10).join("- " + caution for caution in cautions)}

Sample related exception rows:
{sample_exception_rows}

Source-to-target reconciliation checkpoints:
{reconciliation_rows}

Pipeline health context:
{pipeline_rows}

Return:
1. Plain-English summary
2. Most likely layer to check first
3. Evidence supporting that recommendation
4. Suggested SQL/support checks
5. What not to assume yet
"""

    return prompt.strip()


# -----------------------------------------------------------------------------
# Sidebar filters
# -----------------------------------------------------------------------------

valid_dates = rca_context_df["production_date"].dropna()

if valid_dates.empty:
    st.warning("RCA context does not contain valid production dates.")
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


severity_options = get_string_options(rca_context_df, "severity")
exception_type_options = get_string_options(rca_context_df, "exception_type")
region_options = get_string_options(rca_context_df, "region")
facility_options = get_string_options(rca_context_df, "facility_id")
scenario_options = get_string_options(rca_context_df, "scenario_id")

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

selected_scenarios = st.sidebar.multiselect(
    "Scenario ID",
    options=scenario_options,
    default=[],
    help="Leave blank to include all scenarios.",
)


# -----------------------------------------------------------------------------
# Apply filters
# -----------------------------------------------------------------------------

filtered_rca_df = rca_context_df.copy()

filtered_rca_df = filtered_rca_df[
    (filtered_rca_df["production_date"].dt.date >= start_date)
    & (filtered_rca_df["production_date"].dt.date <= end_date)
]

filtered_rca_df = apply_multiselect_filter(
    filtered_rca_df,
    "severity",
    selected_severities,
)

filtered_rca_df = apply_multiselect_filter(
    filtered_rca_df,
    "exception_type",
    selected_exception_types,
)

filtered_rca_df = apply_multiselect_filter(
    filtered_rca_df,
    "region",
    selected_regions,
)

if selected_facilities:
    filtered_rca_df = apply_multiselect_filter(
        filtered_rca_df,
        "facility_id",
        selected_facilities,
    )

if selected_scenarios:
    filtered_rca_df = apply_multiselect_filter(
        filtered_rca_df,
        "scenario_id",
        selected_scenarios,
    )

if filtered_rca_df.empty:
    st.warning("No RCA context rows match the selected filters.")
    st.stop()


# -----------------------------------------------------------------------------
# KPI cards
# -----------------------------------------------------------------------------

rca_group_count = len(filtered_rca_df)
total_exceptions = int(filtered_rca_df["exception_count"].fillna(0).sum())
total_affected_readings = int(filtered_rca_df["affected_reading_count"].fillna(0).sum())
total_affected_meters = int(filtered_rca_df["affected_meter_count"].fillna(0).sum())
scenario_count = filtered_rca_df["scenario_id"].dropna().astype(str).nunique()

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

with kpi1:
    st.metric("RCA Groups", f"{rca_group_count:,}")

with kpi2:
    st.metric("Exception Records", f"{total_exceptions:,}")

with kpi3:
    st.metric("Affected Readings", f"{total_affected_readings:,}")

with kpi4:
    st.metric("Affected Meters", f"{total_affected_meters:,}")

with kpi5:
    st.metric("Known Scenarios", f"{scenario_count:,}")


# -----------------------------------------------------------------------------
# Select RCA group
# -----------------------------------------------------------------------------

st.subheader("Select RCA Context")

select_df = filtered_rca_df.copy()

select_df["rca_label"] = (
    select_df["production_date"].dt.date.astype(str)
    + " | "
    + select_df["facility_id"].fillna("UNKNOWN").astype(str)
    + " | "
    + select_df["exception_type"].fillna("UNKNOWN").astype(str)
    + " | "
    + select_df["severity"].fillna("UNKNOWN").astype(str)
    + " | exceptions="
    + select_df["exception_count"].fillna(0).astype(int).astype(str)
)

select_options = (
    select_df[["exception_group_id", "rca_label"]]
    .drop_duplicates()
    .head(1000)
)

label_by_group_id = dict(
    zip(
        select_options["exception_group_id"],
        select_options["rca_label"],
    )
)

selected_exception_group_id = st.selectbox(
    "RCA group",
    options=select_options["exception_group_id"].tolist(),
    format_func=lambda value: label_by_group_id.get(value, value),
)

selected_rows = filtered_rca_df[
    filtered_rca_df["exception_group_id"] == selected_exception_group_id
]

if selected_rows.empty:
    st.warning("Selected RCA group was not found.")
    st.stop()

selected_row = selected_rows.iloc[0]


# -----------------------------------------------------------------------------
# Related context
# -----------------------------------------------------------------------------

selected_production_date = selected_row.get("production_date")
selected_facility_id = safe_value(selected_row.get("facility_id"), "")
selected_exception_type = safe_value(selected_row.get("exception_type"), "")

related_exception_df = exception_detail_df.copy()

if not related_exception_df.empty:
    related_exception_df = related_exception_df[
        (related_exception_df["facility_id"].fillna("").astype(str) == selected_facility_id)
        & (
            related_exception_df["exception_type"]
            .fillna("")
            .astype(str)
            == selected_exception_type
        )
    ]

    if isinstance(selected_production_date, pd.Timestamp):
        related_exception_df = related_exception_df[
            related_exception_df["production_date"].dt.date
            == selected_production_date.date()
        ]


likely_layer = infer_likely_layer(
    exception_type=selected_row.get("exception_type"),
    device_statuses=selected_row.get("device_statuses_text"),
    communication_statuses=selected_row.get("communication_statuses_text"),
    signal_qualities=selected_row.get("signal_qualities_text"),
    pipeline_context=pipeline_health_df,
)

suggested_checks = build_suggested_checks(
    exception_type=selected_row.get("exception_type"),
    likely_layer=likely_layer,
)

cautions = build_what_not_to_assume(
    exception_type=selected_row.get("exception_type"),
    likely_layer=likely_layer,
)

plain_english_summary = build_plain_english_summary(
    selected_row=selected_row,
    likely_layer=likely_layer,
)

prompt_preview = build_prompt_preview(
    selected_row=selected_row,
    likely_layer=likely_layer,
    suggested_checks=suggested_checks,
    cautions=cautions,
    related_exceptions_df=related_exception_df,
    reconciliation_df=reconciliation_df,
    pipeline_health_df=pipeline_health_df,
)


# -----------------------------------------------------------------------------
# RCA output
# -----------------------------------------------------------------------------

st.subheader("Facts-Only RCA Summary")

summary_col1, summary_col2, summary_col3 = st.columns(3)

with summary_col1:
    st.metric("Facility", safe_value(selected_row.get("facility_id")))
    st.metric("Exception Type", safe_value(selected_row.get("exception_type")))

with summary_col2:
    st.metric("Severity", safe_value(selected_row.get("severity")))
    st.metric("Exception Count", f"{safe_int(selected_row.get('exception_count')):,}")

with summary_col3:
    st.metric("Affected Meters", f"{safe_int(selected_row.get('affected_meter_count')):,}")
    st.metric("Likely Layer", likely_layer)

st.markdown("#### Plain-English Summary")
st.info(plain_english_summary)

st.markdown("#### Most Likely Layer to Check First")
st.success(likely_layer)

st.markdown("#### Evidence Used")

evidence_data = {
    "production_date": format_date(selected_row.get("production_date")),
    "facility_id": safe_value(selected_row.get("facility_id")),
    "facility_name": safe_value(selected_row.get("facility_name")),
    "region": safe_value(selected_row.get("region")),
    "basin": safe_value(selected_row.get("basin")),
    "exception_type": safe_value(selected_row.get("exception_type")),
    "severity": safe_value(selected_row.get("severity")),
    "exception_count": safe_int(selected_row.get("exception_count")),
    "affected_reading_count": safe_int(selected_row.get("affected_reading_count")),
    "affected_meter_count": safe_int(selected_row.get("affected_meter_count")),
    "source_systems": safe_value(selected_row.get("source_systems_text")),
    "polling_platforms": safe_value(selected_row.get("polling_platforms_text")),
    "device_statuses": safe_value(selected_row.get("device_statuses_text")),
    "communication_statuses": safe_value(selected_row.get("communication_statuses_text")),
    "signal_qualities": safe_value(selected_row.get("signal_qualities_text")),
    "scenario_id": safe_value(selected_row.get("scenario_id")),
    "scenario_name": safe_value(selected_row.get("scenario_name")),
    "expected_root_cause": safe_value(selected_row.get("expected_root_cause")),
    "recommended_next_step": safe_value(selected_row.get("recommended_next_step")),
}

st.json(evidence_data)

st.markdown("#### Suggested SQL / Support Checks")

for check in suggested_checks:
    st.write(f"- {check}")

st.markdown("#### What Not to Assume Yet")

for caution in cautions:
    st.write(f"- {caution}")


# -----------------------------------------------------------------------------
# Supporting tables
# -----------------------------------------------------------------------------

st.subheader("Related Exception Detail")

if related_exception_df.empty:
    st.warning("No related exception detail rows found for the selected RCA group.")
else:
    related_exception_display_columns = [
        "exception_id",
        "rule_id",
        "business_reason",
        "raw_reading_id",
        "meter_id",
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
    ]

    available_related_exception_columns = [
        column_name
        for column_name in related_exception_display_columns
        if column_name in related_exception_df.columns
    ]

    st.dataframe(
        related_exception_df[available_related_exception_columns].head(250),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Pipeline Health Context")

if pipeline_health_df.empty:
    st.warning("No pipeline health context returned from BigQuery.")
else:
    pipeline_display_columns = [
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

    available_pipeline_columns = [
        column_name
        for column_name in pipeline_display_columns
        if column_name in pipeline_health_df.columns
    ]

    st.dataframe(
        pipeline_health_df[available_pipeline_columns],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Source-to-Target Context")

if reconciliation_df.empty:
    st.warning("No reconciliation context returned from BigQuery.")
else:
    reconciliation_display_columns = [
        "stage_order",
        "layer",
        "object_name",
        "row_count",
        "metric_type",
        "notes",
    ]

    available_reconciliation_columns = [
        column_name
        for column_name in reconciliation_display_columns
        if column_name in reconciliation_df.columns
    ]

    st.dataframe(
        reconciliation_df[available_reconciliation_columns],
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------------------------------------------------------
# Future Gemini prompt preview
# -----------------------------------------------------------------------------

st.subheader("Future Gemini Prompt Preview")

st.caption(
    "This prompt is not sent anywhere yet. It is included so the future Gemini integration "
    "has a safe, structured, facts-only request template."
)

st.text_area(
    "Prompt preview",
    value=prompt_preview,
    height=500,
)


# -----------------------------------------------------------------------------
# Optional analyst review mockup
# -----------------------------------------------------------------------------

st.subheader("Human-in-the-Loop Review Mockup")

review_col1, review_col2 = st.columns(2)

with review_col1:
    analyst_decision = st.selectbox(
        "Analyst decision",
        options=[
            "Not reviewed",
            "Accept suggested checks",
            "Needs more investigation",
            "Reject / not enough evidence",
        ],
    )

with review_col2:
    analyst_priority = st.selectbox(
        "Priority",
        options=[
            "Medium",
            "High",
            "Low",
            "Critical",
        ],
    )

analyst_notes = st.text_area(
    "Analyst notes",
    value="",
    placeholder="Future PostgreSQL writeback will store notes, assignments, review status, and audit history.",
    height=120,
)

st.caption(
    "Writeback is not enabled yet. The project plan reserves PostgreSQL for exception status, notes, "
    "assignments, AI review history, and audit trail."
)


# -----------------------------------------------------------------------------
# Metadata
# -----------------------------------------------------------------------------

with st.expander("BigQuery publish metadata"):
    metadata_columns = [
        "_bigquery_publish_run_id",
        "_bigquery_published_at_utc",
    ]

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

    if not exception_detail_df.empty:
        available_exception_metadata_columns = [
            column_name
            for column_name in metadata_columns
            if column_name in exception_detail_df.columns
        ]

        if available_exception_metadata_columns:
            st.write("Exception detail publish metadata")
            st.dataframe(
                exception_detail_df[
                    available_exception_metadata_columns
                ].drop_duplicates(),
                use_container_width=True,
                hide_index=True,
            )
