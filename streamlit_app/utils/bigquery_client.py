"""
MeterFlow IQ - BigQuery Client Utilities

Authentication order:
1. Streamlit secrets service account info, if present. Useful for platforms that
   allow service account JSON secrets.
2. Application Default Credentials (ADC). This is the preferred path for:
   - local development after `gcloud auth application-default login`
   - Cloud Run with an attached service account
3. Temporary GCP_ACCESS_TOKEN fallback for emergency local MVP use only.

For deployment, prefer Cloud Run with a user-managed service account attached.
Do not commit tokens, service account keys, or secrets.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.auth.exceptions import DefaultCredentialsError
from google.auth import default as google_auth_default
from google.cloud import bigquery
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as AccessTokenCredentials


# -----------------------------------------------------------------------------
# Environment / constants
# -----------------------------------------------------------------------------

load_dotenv()

DEFAULT_PROJECT_ID = "project-616f71e8-6bb8-4927-978"
DEFAULT_DATASET_ID = "meterflow_iq_curated"
DEFAULT_LOCATION = "US"

BIGQUERY_SCOPES = ["https://www.googleapis.com/auth/bigquery"]


# -----------------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------------

def _get_secret_path(section: str, key: str) -> Optional[str]:
    """
    Safely read st.secrets[section][key].

    This must not fail when running outside Streamlit, when no secrets.toml
    exists, or when a deployed environment uses only environment variables.
    """
    try:
        section_value = st.secrets.get(section, None)
        if section_value is None:
            return None

        value = section_value.get(key, None)
        if value is None:
            return None

        return str(value).strip()
    except Exception:
        return None


def _get_secret_top_level(key: str) -> Optional[str]:
    """
    Safely read st.secrets[key].
    """
    try:
        value = st.secrets.get(key, None)
        if value is None:
            return None

        return str(value).strip()
    except Exception:
        return None


def _get_config_value(
    env_names: Iterable[str],
    secret_paths: Iterable[Tuple[str, str]] = (),
    secret_top_level_names: Iterable[str] = (),
    default_value: str = "",
) -> str:
    """
    Read configuration from Streamlit secrets first, then environment variables.
    """
    for section, key in secret_paths:
        value = _get_secret_path(section, key)
        if value:
            return value

    for key in secret_top_level_names:
        value = _get_secret_top_level(key)
        if value:
            return value

    for env_name in env_names:
        value = os.getenv(env_name, "").strip()
        if value:
            return value

    return default_value


PROJECT_ID = _get_config_value(
    env_names=[
        "BIGQUERY_PROJECT_ID",
        "GCP_PROJECT_ID",
        "GOOGLE_CLOUD_PROJECT",
        "PROJECT_ID",
    ],
    secret_paths=[
        ("bigquery", "project_id"),
    ],
    secret_top_level_names=[
        "BIGQUERY_PROJECT_ID",
        "GCP_PROJECT_ID",
    ],
    default_value=DEFAULT_PROJECT_ID,
)

DATASET_ID = _get_config_value(
    env_names=[
        "BIGQUERY_DATASET_ID",
        "BIGQUERY_DATASET",
        "BQ_DATASET_ID",
        "BQ_DATASET",
    ],
    secret_paths=[
        ("bigquery", "dataset_id"),
    ],
    secret_top_level_names=[
        "BIGQUERY_DATASET_ID",
        "BIGQUERY_DATASET",
    ],
    default_value=DEFAULT_DATASET_ID,
)

LOCATION = _get_config_value(
    env_names=[
        "BIGQUERY_LOCATION",
        "BQ_LOCATION",
        "GCP_LOCATION",
    ],
    secret_paths=[
        ("bigquery", "location"),
    ],
    secret_top_level_names=[
        "BIGQUERY_LOCATION",
        "BQ_LOCATION",
    ],
    default_value=DEFAULT_LOCATION,
)

# Backward-compatible aliases in case existing pages import these names.
BQ_PROJECT_ID = PROJECT_ID
BQ_DATASET_ID = DATASET_ID
BQ_LOCATION = LOCATION


# -----------------------------------------------------------------------------
# Authentication helpers
# -----------------------------------------------------------------------------

def _get_service_account_info_from_streamlit_secrets() -> Optional[Dict[str, Any]]:
    """
    Return service account info from st.secrets["gcp_service_account"], if present.

    This is optional. In this project, Google Cloud org policy currently blocks
    service account key creation, so Cloud Run + ADC is the target deploy path.
    """
    try:
        info = st.secrets.get("gcp_service_account", None)
        if not info:
            return None

        info_dict = dict(info)

        required_keys = {
            "type",
            "project_id",
            "private_key",
            "client_email",
            "token_uri",
        }

        if not required_keys.issubset(set(info_dict.keys())):
            return None

        return info_dict
    except Exception:
        return None


def _get_temporary_access_token() -> str:
    """
    Return temporary access token fallback from secrets/env.

    This is only for emergency local use. It should not be needed for Cloud Run.
    """
    token = _get_secret_top_level("GCP_ACCESS_TOKEN")
    if token:
        return token

    return os.getenv("GCP_ACCESS_TOKEN", "").strip()


@st.cache_resource(show_spinner=False)
def _build_credentials_and_auth_mode() -> Tuple[Optional[Any], str]:
    """
    Build credentials and return (credentials, auth_mode).

    credentials can be None. Passing None to bigquery.Client lets the Google
    client library use ADC automatically.
    """
    service_account_info = _get_service_account_info_from_streamlit_secrets()

    if service_account_info:
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=BIGQUERY_SCOPES,
        )
        return credentials, "streamlit_service_account_secret"

    try:
        credentials, adc_project_id = google_auth_default(scopes=BIGQUERY_SCOPES)

        # If ADC exists, use it. Project is still controlled by PROJECT_ID.
        _ = adc_project_id
        return credentials, "application_default_credentials"
    except DefaultCredentialsError:
        pass
    except Exception:
        pass

    access_token = _get_temporary_access_token()
    if access_token:
        credentials = AccessTokenCredentials(token=access_token)
        return credentials, "temporary_access_token"

    return None, "application_default_credentials_unresolved"


def get_bigquery_auth_mode() -> str:
    """
    Return the auth mode selected by the client helper.
    """
    _, auth_mode = _build_credentials_and_auth_mode()
    return auth_mode


def is_temporary_token_mode() -> bool:
    """
    Return True when the client is using temporary GCP_ACCESS_TOKEN fallback.
    """
    return get_bigquery_auth_mode() == "temporary_access_token"

def get_environment_label() -> str:
    """
    Return a short environment/auth label for the Streamlit UI.

    Backward-compatible helper used by streamlit_app/app.py.
    """
    auth_mode = get_bigquery_auth_mode()

    if os.getenv("K_SERVICE"):
        runtime_label = "Cloud Run"
    else:
        runtime_label = "Local"

    if auth_mode == "application_default_credentials":
        return f"{runtime_label} / ADC"

    if auth_mode == "streamlit_service_account_secret":
        return f"{runtime_label} / Streamlit service account secret"

    if auth_mode == "temporary_access_token":
        return f"{runtime_label} / temporary access token"

    return f"{runtime_label} / auth unresolved"


def get_bigquery_auth_caption() -> str:
    """
    Return a short user-facing BigQuery authentication caption.
    """
    auth_label = get_environment_label()

    if "Cloud Run" in auth_label and "ADC" in auth_label:
        return (
            "BigQuery auth: Cloud Run / ADC using keyless service-account "
            "identity."
        )

    if "temporary access token" in auth_label:
        return "BigQuery auth: temporary token fallback for local testing only."

    if "ADC" in auth_label:
        return "BigQuery auth: Application Default Credentials."

    return "BigQuery auth is configured through the app environment."


def token_present() -> bool:
    """
    Return whether BigQuery authentication appears available.

    Backward-compatible helper used by streamlit_app/app.py.

    Historical meaning:
        True when GCP_ACCESS_TOKEN was configured.

    New meaning:
        True when any supported BigQuery auth path is available:
        - Cloud Run / local Application Default Credentials
        - Streamlit service account secret
        - temporary GCP_ACCESS_TOKEN fallback
    """
    auth_mode = get_bigquery_auth_mode()

    return auth_mode in {
        "application_default_credentials",
        "streamlit_service_account_secret",
        "temporary_access_token",
    }


# -----------------------------------------------------------------------------
# BigQuery client and query helpers
# -----------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def get_bigquery_client() -> bigquery.Client:
    """
    Return a cached BigQuery client.

    For Cloud Run, ADC should resolve to the service account attached to the
    Cloud Run service. For local development, ADC should resolve after:
        gcloud auth application-default login
    """
    credentials, auth_mode = _build_credentials_and_auth_mode()

    if credentials is None and auth_mode == "application_default_credentials_unresolved":
        # Still let bigquery.Client try ADC so the real Google error is visible.
        return bigquery.Client(project=PROJECT_ID, location=LOCATION)

    return bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials,
        location=LOCATION,
    )


# Backward-compatible aliases.
get_client = get_bigquery_client
get_bq_client = get_bigquery_client


def fully_qualified_table(table_or_view_name: str) -> str:
    """
    Return a backtick-quoted BigQuery table/view identifier.

    Example:
        `project.dataset.view_name`
    """
    safe_name = table_or_view_name.replace("`", "")
    return f"`{PROJECT_ID}.{DATASET_ID}.{safe_name}`"


# Backward-compatible aliases.
qualified_table = fully_qualified_table
table_ref = fully_qualified_table
bq_table = fully_qualified_table
fq_table = fully_qualified_table


@st.cache_data(ttl=300, show_spinner=False)
def _query_to_dataframe_cached(sql: str) -> pd.DataFrame:
    """
    Cached query execution for SQL strings with no parameters.
    """
    client = get_bigquery_client()

    query_job = client.query(
        sql,
        location=LOCATION,
    )

    return query_job.result().to_dataframe(
        create_bqstorage_client=False,
    )


def query_to_dataframe(
    sql: str,
    query_parameters: Optional[Sequence[bigquery.ScalarQueryParameter]] = None,
) -> pd.DataFrame:
    """
    Run a BigQuery SQL query and return a pandas DataFrame.

    Uses cached execution when no query parameters are supplied.
    """
    if not query_parameters:
        return _query_to_dataframe_cached(sql)

    client = get_bigquery_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=list(query_parameters),
    )

    query_job = client.query(
        sql,
        job_config=job_config,
        location=LOCATION,
    )

    return query_job.result().to_dataframe(
        create_bqstorage_client=False,
    )


# Backward-compatible aliases.
run_query = query_to_dataframe
read_query = query_to_dataframe
query_bigquery = query_to_dataframe
bq_query = query_to_dataframe


def test_bigquery_connection() -> Dict[str, Any]:
    """
    Execute a minimal BigQuery query and return connection metadata.
    """
    sql = f"""
    SELECT
        1 AS connection_test,
        CURRENT_TIMESTAMP() AS checked_at_utc,
        '{PROJECT_ID}' AS configured_project_id,
        '{DATASET_ID}' AS configured_dataset_id,
        '{LOCATION}' AS configured_location,
        '{get_bigquery_auth_mode()}' AS auth_mode
    """

    df = query_to_dataframe(sql)

    if df.empty:
        raise RuntimeError("BigQuery connection test returned no rows.")

    row = df.iloc[0].to_dict()

    return {
        "connection_test": row.get("connection_test"),
        "checked_at_utc": row.get("checked_at_utc"),
        "project_id": row.get("configured_project_id"),
        "dataset_id": row.get("configured_dataset_id"),
        "location": row.get("configured_location"),
        "auth_mode": row.get("auth_mode"),
    }


def get_bigquery_status_message() -> str:
    """
    Return a short status message suitable for Streamlit display.
    """
    auth_mode = get_bigquery_auth_mode()

    if auth_mode == "application_default_credentials":
        return "BigQuery authentication is using Application Default Credentials."

    if auth_mode == "streamlit_service_account_secret":
        return "BigQuery authentication is using Streamlit service account secrets."

    if auth_mode == "temporary_access_token":
        return (
            "BigQuery authentication is using temporary GCP_ACCESS_TOKEN fallback. "
            "This is okay for local testing but should not be used for deployment."
        )

    return (
        "BigQuery authentication has not resolved yet. Configure local ADC with "
        "`gcloud auth application-default login` or deploy on Cloud Run with an "
        "attached service account."
    )


def clear_bigquery_cache() -> None:
    """
    Clear cached BigQuery client/query results.
    """
    try:
        _query_to_dataframe_cached.clear()
    except Exception:
        pass

    try:
        get_bigquery_client.clear()
    except Exception:
        pass

    try:
        _build_credentials_and_auth_mode.clear()
    except Exception:
        pass


# Backward-compatible aliases.
clear_bq_cache = clear_bigquery_cache
clear_cache = clear_bigquery_cache
refresh_bigquery_cache = clear_bigquery_cache
clear_streamlit_caches = clear_bigquery_cache
