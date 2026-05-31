"""
MeterFlow IQ - BigQuery helper functions for Streamlit.

This module centralizes:
- .env loading
- BigQuery client creation
- BigQuery SQL execution
- fully qualified BigQuery table/view name construction
- cache clearing

Current local MVP authentication:
Uses a temporary Google access token stored in local .env as GCP_ACCESS_TOKEN.

Do not commit .env or any access token.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.credentials import Credentials


# Load local .env from the repo/project context.
load_dotenv()


@dataclass(frozen=True)
class BigQueryConfig:
    project_id: str
    dataset_id: str
    location: str
    access_token: str


def get_bigquery_config() -> BigQueryConfig:
    """
    Read BigQuery connection settings from environment variables.
    """
    return BigQueryConfig(
        project_id=os.getenv("BIGQUERY_PROJECT_ID", "").strip(),
        dataset_id=os.getenv("BIGQUERY_DATASET_ID", "").strip(),
        location=os.getenv("BIGQUERY_LOCATION", "US").strip(),
        access_token=os.getenv("GCP_ACCESS_TOKEN", "").strip(),
    )


def validate_config(config: BigQueryConfig) -> List[str]:
    """
    Return missing required environment variable names.
    """
    missing_values: List[str] = []

    if not config.project_id:
        missing_values.append("BIGQUERY_PROJECT_ID")

    if not config.dataset_id:
        missing_values.append("BIGQUERY_DATASET_ID")

    if not config.location:
        missing_values.append("BIGQUERY_LOCATION")

    if not config.access_token:
        missing_values.append("GCP_ACCESS_TOKEN")

    return missing_values


@st.cache_resource(show_spinner=False)
def get_bigquery_client(
    project_id: str,
    location: str,
    access_token: str,
) -> bigquery.Client:
    """
    Create and cache a BigQuery client for the current Streamlit session.

    The access token is passed as a cache input. If the token changes,
    restart Streamlit to create a clean session/client.
    """
    credentials = Credentials(
        token=access_token,
        quota_project_id=project_id,
    )

    return bigquery.Client(
        project=project_id,
        credentials=credentials,
        location=location,
    )


def get_client() -> bigquery.Client:
    """
    Validate environment config and return a BigQuery client.
    """
    config = get_bigquery_config()
    missing_values = validate_config(config)

    if missing_values:
        st.error(
            "Missing required environment values: "
            + ", ".join(missing_values)
        )
        st.stop()

    return get_bigquery_client(
        project_id=config.project_id,
        location=config.location,
        access_token=config.access_token,
    )


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    """
    Run a BigQuery query and return a pandas DataFrame.

    Results are cached for 5 minutes to keep the local app responsive.
    """
    config = get_bigquery_config()
    client = get_client()

    query_job = client.query(
        sql,
        location=config.location,
    )

    return query_job.result().to_dataframe()


def fq_table(table_or_view_name: str) -> str:
    """
    Return a fully qualified BigQuery table/view reference wrapped in backticks.

    Example:
    `project-id.dataset_id.table_name`
    """
    config = get_bigquery_config()

    return (
        f"`{config.project_id}."
        f"{config.dataset_id}."
        f"{table_or_view_name}`"
    )


def get_environment_label() -> str:
    """
    Return a display label for the current BigQuery environment.
    """
    config = get_bigquery_config()

    project_display = config.project_id if config.project_id else "not configured"
    dataset_display = config.dataset_id if config.dataset_id else "not configured"
    location_display = config.location if config.location else "not configured"

    return (
        f"Project: `{project_display}` | "
        f"Dataset: `{dataset_display}` | "
        f"Location: `{location_display}`"
    )


def token_present() -> bool:
    """
    Return True when a local GCP access token is configured.
    """
    config = get_bigquery_config()
    return bool(config.access_token)


def clear_streamlit_caches() -> None:
    """
    Clear Streamlit query/client caches.

    Use this after refreshing a GCP token or republishing BigQuery tables.
    """
    st.cache_data.clear()
    st.cache_resource.clear()