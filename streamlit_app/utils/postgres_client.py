from __future__ import annotations

import json
import math
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

try:
    import psycopg
except ImportError:  # pragma: no cover - allows read-only app mode if dependency is absent
    psycopg = None  # type: ignore[assignment]

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]


REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = REPO_ROOT / ".env"

POSTGRES_REQUIRED_ENV = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_SSLMODE",
)

APP_STATE_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS exception_triage_status (
        triage_id BIGSERIAL PRIMARY KEY,
        exception_group_key TEXT NOT NULL,
        exception_id TEXT NULL,
        production_date DATE NULL,
        facility_id TEXT NULL,
        facility_name TEXT NULL,
        exception_type TEXT NULL,
        severity TEXT NULL,
        status TEXT NOT NULL DEFAULT 'Not reviewed',
        priority TEXT NOT NULL DEFAULT 'Medium',
        assigned_to TEXT NULL,
        updated_by TEXT NOT NULL DEFAULT 'streamlit_user',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (exception_group_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS exception_notes (
        note_id BIGSERIAL PRIMARY KEY,
        exception_group_key TEXT NOT NULL,
        exception_id TEXT NULL,
        note_text TEXT NOT NULL,
        created_by TEXT NOT NULL DEFAULT 'streamlit_user',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ai_rca_review_log (
        review_id BIGSERIAL PRIMARY KEY,
        exception_group_key TEXT NOT NULL,
        exception_id TEXT NULL,
        ai_provider TEXT NULL,
        prompt_text TEXT NULL,
        response_text TEXT NULL,
        reviewer_decision TEXT NOT NULL DEFAULT 'Not reviewed',
        accepted_flag BOOLEAN NULL,
        reviewer_comments TEXT NULL,
        reviewed_by TEXT NOT NULL DEFAULT 'streamlit_user',
        reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS app_user_activity (
        activity_id BIGSERIAL PRIMARY KEY,
        user_id TEXT NOT NULL DEFAULT 'streamlit_user',
        action_type TEXT NOT NULL,
        object_type TEXT NOT NULL,
        object_id TEXT NULL,
        activity_detail JSONB NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_exception_triage_status_group_key
        ON exception_triage_status (exception_group_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_exception_notes_group_key
        ON exception_notes (exception_group_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ai_rca_review_log_group_key
        ON ai_rca_review_log (exception_group_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_app_user_activity_object
        ON app_user_activity (object_type, object_id);
    """,
]


def _load_local_env() -> None:
    """
    Load local .env for developer mode.

    In deployed Cloud Run mode, .env is not shipped because it is ignored by
    .dockerignore. Environment variables should come from Cloud Run settings.
    """
    if load_dotenv is not None and ENV_PATH.exists():
        load_dotenv(ENV_PATH, override=True)


def _env(name: str, default: str | None = None) -> str | None:
    _load_local_env()
    value = os.getenv(name, default)

    if value is None:
        return None

    value = str(value).strip()
    return value if value else None


def _truthy(value: str | None) -> bool:
    if value is None:
        return False

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _missing_postgres_config() -> list[str]:
    return [name for name in POSTGRES_REQUIRED_ENV if not _env(name)]


def postgres_writeback_enabled() -> bool:
    """
    Return True only when PostgreSQL writeback is explicitly enabled and configured.
    """
    if not _truthy(_env("POSTGRES_WRITEBACK_ENABLED", "false")):
        return False

    if psycopg is None:
        return False

    return not _missing_postgres_config()


def get_postgres_auth_caption() -> str:
    """
    Small safe caption for Streamlit sidebars / writeback sections.
    Does not expose secrets.
    """
    if not _truthy(_env("POSTGRES_WRITEBACK_ENABLED", "false")):
        return "PostgreSQL writeback: disabled. Review page is running in read-only mode."

    if psycopg is None:
        return "PostgreSQL writeback: enabled but psycopg is not installed."

    missing = _missing_postgres_config()
    if missing:
        return f"PostgreSQL writeback: enabled but missing config: {', '.join(missing)}."

    host = _env("POSTGRES_HOST", "")
    db_name = _env("POSTGRES_DB", "")
    return f"PostgreSQL writeback: enabled using Azure PostgreSQL app-state database {db_name} on {host}."


def _get_connection() -> psycopg.Connection:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Add psycopg[binary] to requirements.txt.")

    missing = _missing_postgres_config()
    if missing:
        raise RuntimeError(f"Missing PostgreSQL configuration: {', '.join(missing)}")

    return psycopg.connect(
        host=_env("POSTGRES_HOST"),
        port=int(_env("POSTGRES_PORT", "5432") or "5432"),
        dbname=_env("POSTGRES_DB"),
        user=_env("POSTGRES_USER"),
        password=_env("POSTGRES_PASSWORD"),
        sslmode=_env("POSTGRES_SSLMODE", "require") or "require",
        connect_timeout=20,
    )


def _ensure_app_state_tables(cur: psycopg.Cursor) -> None:
    for statement in APP_STATE_SCHEMA_STATEMENTS:
        cur.execute(statement)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, float) and math.isnan(value):
        return True

    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "nat", "none", "null"}


def _clean_text(value: Any) -> str | None:
    if _is_missing(value):
        return None

    return str(value).strip()


def _clean_date(value: Any) -> date | str | None:
    if _is_missing(value):
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    if hasattr(value, "to_pydatetime"):
        try:
            converted = value.to_pydatetime()
            if isinstance(converted, datetime):
                return converted.date()
        except Exception:
            pass

    text = str(value).strip()
    if " " in text:
        text = text.split(" ")[0]
    if "T" in text:
        text = text.split("T")[0]

    return text


def _json_dumps_safe(value: Mapping[str, Any] | None) -> str:
    if not value:
        return "{}"

    def default_serializer(obj: Any) -> str:
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return str(obj)

    return json.dumps(value, default=default_serializer)


def build_exception_group_key(
    *,
    production_date: Any,
    facility_id: Any,
    exception_type: Any,
    severity: Any,
) -> str:
    """
    Build a stable group key for RCA writeback.

    This intentionally matches the visible RCA group grain:
    production_date | facility_id | exception_type | severity
    """
    production_date_value = _clean_date(production_date)
    return "|".join(
        [
            _clean_text(production_date_value) or "unknown_date",
            _clean_text(facility_id) or "unknown_facility",
            _clean_text(exception_type) or "unknown_exception_type",
            _clean_text(severity) or "unknown_severity",
        ]
    )


def save_ai_rca_review(
    *,
    exception_group_key: str,
    exception_id: Any | None = None,
    production_date: Any | None = None,
    facility_id: Any | None = None,
    facility_name: Any | None = None,
    exception_type: Any | None = None,
    severity: Any | None = None,
    status: str = "Not reviewed",
    priority: str = "Medium",
    assigned_to: Any | None = None,
    note_text: Any | None = None,
    updated_by: str = "streamlit_user",
    prompt_text: Any | None = None,
    response_text: Any | None = None,
    ai_provider: Any | None = None,
    reviewer_decision: Any | None = None,
    accepted_flag: bool | None = None,
    reviewer_comments: Any | None = None,
    activity_detail: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Save the human-in-the-loop AI RCA review to Azure PostgreSQL.

    Writes:
    - current/upserted review state to exception_triage_status
    - optional analyst note to exception_notes
    - review history row to ai_rca_review_log
    - audit activity row to app_user_activity
    """
    if not postgres_writeback_enabled():
        return {
            "saved": False,
            "writeback_enabled": False,
            "message": get_postgres_auth_caption(),
        }

    clean_group_key = _clean_text(exception_group_key)
    if not clean_group_key:
        raise ValueError("exception_group_key is required for PostgreSQL writeback.")

    clean_status = _clean_text(status) or "Not reviewed"
    clean_priority = _clean_text(priority) or "Medium"
    clean_updated_by = _clean_text(updated_by) or "streamlit_user"
    clean_note = _clean_text(note_text)
    clean_reviewer_decision = _clean_text(reviewer_decision) or clean_status
    clean_reviewer_comments = _clean_text(reviewer_comments) or clean_note

    with _get_connection() as conn:
        with conn.cursor() as cur:
            _ensure_app_state_tables(cur)

            cur.execute(
                """
                INSERT INTO exception_triage_status (
                    exception_group_key,
                    exception_id,
                    production_date,
                    facility_id,
                    facility_name,
                    exception_type,
                    severity,
                    status,
                    priority,
                    assigned_to,
                    updated_by
                )
                VALUES (
                    %(exception_group_key)s,
                    %(exception_id)s,
                    %(production_date)s,
                    %(facility_id)s,
                    %(facility_name)s,
                    %(exception_type)s,
                    %(severity)s,
                    %(status)s,
                    %(priority)s,
                    %(assigned_to)s,
                    %(updated_by)s
                )
                ON CONFLICT (exception_group_key)
                DO UPDATE SET
                    exception_id = EXCLUDED.exception_id,
                    production_date = EXCLUDED.production_date,
                    facility_id = EXCLUDED.facility_id,
                    facility_name = EXCLUDED.facility_name,
                    exception_type = EXCLUDED.exception_type,
                    severity = EXCLUDED.severity,
                    status = EXCLUDED.status,
                    priority = EXCLUDED.priority,
                    assigned_to = EXCLUDED.assigned_to,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = NOW();
                """,
                {
                    "exception_group_key": clean_group_key,
                    "exception_id": _clean_text(exception_id),
                    "production_date": _clean_date(production_date),
                    "facility_id": _clean_text(facility_id),
                    "facility_name": _clean_text(facility_name),
                    "exception_type": _clean_text(exception_type),
                    "severity": _clean_text(severity),
                    "status": clean_status,
                    "priority": clean_priority,
                    "assigned_to": _clean_text(assigned_to),
                    "updated_by": clean_updated_by,
                },
            )

            note_inserted = False
            if clean_note:
                cur.execute(
                    """
                    INSERT INTO exception_notes (
                        exception_group_key,
                        exception_id,
                        note_text,
                        created_by
                    )
                    VALUES (%s, %s, %s, %s);
                    """,
                    (
                        clean_group_key,
                        _clean_text(exception_id),
                        clean_note,
                        clean_updated_by,
                    ),
                )
                note_inserted = True

            cur.execute(
                """
                INSERT INTO ai_rca_review_log (
                    exception_group_key,
                    exception_id,
                    ai_provider,
                    prompt_text,
                    response_text,
                    reviewer_decision,
                    accepted_flag,
                    reviewer_comments,
                    reviewed_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    clean_group_key,
                    _clean_text(exception_id),
                    _clean_text(ai_provider),
                    _clean_text(prompt_text),
                    _clean_text(response_text),
                    clean_reviewer_decision,
                    accepted_flag,
                    clean_reviewer_comments,
                    clean_updated_by,
                ),
            )

            detail = {
                "status": clean_status,
                "priority": clean_priority,
                "assigned_to": _clean_text(assigned_to),
                "note_inserted": note_inserted,
            }
            if activity_detail:
                detail.update(dict(activity_detail))

            cur.execute(
                """
                INSERT INTO app_user_activity (
                    user_id,
                    action_type,
                    object_type,
                    object_id,
                    activity_detail
                )
                VALUES (%s, %s, %s, %s, %s::jsonb);
                """,
                (
                    clean_updated_by,
                    "SAVE_AI_RCA_REVIEW",
                    "exception_group",
                    clean_group_key,
                    _json_dumps_safe(detail),
                ),
            )

        conn.commit()

    return {
        "saved": True,
        "writeback_enabled": True,
        "exception_group_key": clean_group_key,
        "note_inserted": note_inserted,
        "message": "AI RCA review saved to PostgreSQL app-state tables.",
    }
