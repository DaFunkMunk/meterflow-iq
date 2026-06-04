"""
MeterFlow IQ - Streamlit Command Center

Main landing page.

The Streamlit app reads curated BigQuery views produced by the Databricks
Gold publisher. It is the support analyst investigation surface for pipeline
health, exceptions, reconciliation, facility KPIs, and RCA context.
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="MeterFlowIQ - Command Center",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# project imports go AFTER st.set_page_config
from utils.bigquery_client import (
    get_environment_label,
    token_present,
)

st.title("MeterFlow IQ - Command Center")
st.caption(
    "Operational data quality command center powered by Databricks Gold outputs "
    "published to BigQuery."
)

st.info(f"BigQuery authentication: {get_environment_label()}")

if token_present():
    st.success(
        "BigQuery authentication is configured. In deployed mode, MeterFlow IQ "
        "uses keyless Cloud Run / ADC with a read-only BigQuery service account."
    )
else:
    st.warning(
        "BigQuery authentication is not configured. Use local ADC for development "
        "or deploy with a Cloud Run service account."
    )

st.markdown(
    """
### What this app is for

MeterFlow IQ is a support analyst command center. It helps answer:

- Is the data pipeline healthy?
- Which facilities, meters, and dates have data-quality exceptions?
- Did records change, disappear, duplicate, or arrive late?
- Are business volume trends real or data-quality driven?
- What facts should an RCA helper summarize?

### Current app sections

Use the left navigation to open:

1. **Pipeline Health** — latest pipeline status, run history, failures, partial loads, and rejected rows.
2. **Exceptions** — data-quality exception triage by facility, meter, rule, severity, source system, and date.
3. **Raw Event Explorer** — MongoDB-derived device/communication/source context.
4. **Reconciliation** — source-to-target row-count checkpoints.
5. **AI RCA Helper** — facts-only RCA summary scaffold.

### Current authentication note

The deployed app uses **Google Cloud Run / Application Default Credentials**
with a read-only BigQuery service account.

No service account key or temporary access token is required for the deployed app.

For local development, use Google Application Default Credentials when available.
A temporary `GCP_ACCESS_TOKEN` fallback may be used only for local testing.
"""
)

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Pipeline path", "Databricks → BigQuery")

with col2:
    st.metric("Investigation source", "BigQuery views")

with col3:
    st.metric("Writeback", "Planned")
