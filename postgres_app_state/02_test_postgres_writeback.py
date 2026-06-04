from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = REPO_ROOT / ".env"

EXPECTED_DB = "meterflow_iq_app_state"

SCHEMA_STATEMENTS = [
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


DEMO_EXCEPTION_GROUP_KEY = "2026-06-03|FAC-002|FUTURE_DATE|Medium"
DEMO_EXCEPTION_ID = "demo-exception-id"


def load_environment() -> None:
    if not ENV_PATH.exists():
        raise FileNotFoundError(f".env file not found at: {ENV_PATH}")

    load_dotenv(ENV_PATH, override=True)


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if value is None or value.strip() == "":
        raise RuntimeError(f"Missing required .env setting: {name}")

    return value.strip()


def get_connection() -> psycopg.Connection:
    load_environment()

    required_names = [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_SSLMODE",
    ]

    missing = [
        name
        for name in required_names
        if os.getenv(name) is None or os.getenv(name, "").strip() == ""
    ]

    if missing:
        raise RuntimeError(f"Missing required .env settings: {', '.join(missing)}")

    db_name = get_required_env("POSTGRES_DB")

    if db_name != EXPECTED_DB:
        raise RuntimeError(
            f"POSTGRES_DB is set to '{db_name}', but this project expects "
            f"'{EXPECTED_DB}'. Update .env and rerun."
        )

    return psycopg.connect(
        host=get_required_env("POSTGRES_HOST"),
        port=int(get_required_env("POSTGRES_PORT")),
        dbname=db_name,
        user=get_required_env("POSTGRES_USER"),
        password=get_required_env("POSTGRES_PASSWORD"),
        sslmode=get_required_env("POSTGRES_SSLMODE"),
        connect_timeout=20,
    )


def create_app_state_tables(cur: psycopg.Cursor) -> None:
    for statement in SCHEMA_STATEMENTS:
        cur.execute(statement)


def write_demo_triage_status(cur: psycopg.Cursor) -> None:
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
            "exception_group_key": DEMO_EXCEPTION_GROUP_KEY,
            "exception_id": DEMO_EXCEPTION_ID,
            "production_date": date(2026, 6, 3),
            "facility_id": "FAC-002",
            "facility_name": "Delaware Gas Processing 02",
            "exception_type": "FUTURE_DATE",
            "severity": "Medium",
            "status": "Investigating",
            "priority": "Medium",
            "assigned_to": "Nate",
            "updated_by": "local_test",
        },
    )


def clean_prior_demo_rows(cur: psycopg.Cursor) -> None:
    """
    Keep repeated local test runs from endlessly adding demo notes/activity rows.
    This only deletes rows created by this local demo key/user.
    """
    cur.execute(
        """
        DELETE FROM exception_notes
        WHERE exception_group_key = %s
          AND created_by = 'local_test';
        """,
        (DEMO_EXCEPTION_GROUP_KEY,),
    )

    cur.execute(
        """
        DELETE FROM app_user_activity
        WHERE object_id = %s
          AND user_id = 'local_test'
          AND action_type = 'POSTGRES_WRITEBACK_TEST';
        """,
        (DEMO_EXCEPTION_GROUP_KEY,),
    )


def write_demo_note(cur: psycopg.Cursor) -> None:
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
            DEMO_EXCEPTION_GROUP_KEY,
            DEMO_EXCEPTION_ID,
            "Test writeback from local Python setup.",
            "local_test",
        ),
    )


def write_demo_activity(cur: psycopg.Cursor) -> None:
    activity_detail: dict[str, Any] = {
        "source": "02_test_postgres_writeback.py",
        "purpose": "local PostgreSQL app-state writeback validation",
        "database": EXPECTED_DB,
    }

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
            "local_test",
            "POSTGRES_WRITEBACK_TEST",
            "exception_group",
            DEMO_EXCEPTION_GROUP_KEY,
            json.dumps(activity_detail),
        ),
    )


def fetch_row_counts(cur: psycopg.Cursor) -> tuple[int, int, int, int]:
    cur.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM exception_triage_status) AS triage_rows,
            (SELECT COUNT(*) FROM exception_notes) AS note_rows,
            (SELECT COUNT(*) FROM ai_rca_review_log) AS ai_review_rows,
            (SELECT COUNT(*) FROM app_user_activity) AS activity_rows;
        """
    )

    row = cur.fetchone()

    if row is None:
        raise RuntimeError("Unable to fetch row counts.")

    return row[0], row[1], row[2], row[3]


def validate_demo_row(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        SELECT
            exception_group_key,
            status,
            priority,
            assigned_to,
            updated_by,
            updated_at
        FROM exception_triage_status
        WHERE exception_group_key = %s;
        """,
        (DEMO_EXCEPTION_GROUP_KEY,),
    )

    row = cur.fetchone()

    if row is None:
        raise RuntimeError("Demo triage row was not found after writeback.")

    print("Demo triage row:")
    print(f"  exception_group_key: {row[0]}")
    print(f"  status:              {row[1]}")
    print(f"  priority:            {row[2]}")
    print(f"  assigned_to:         {row[3]}")
    print(f"  updated_by:          {row[4]}")
    print(f"  updated_at:          {row[5]}")


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            print("Connected to PostgreSQL.")

            cur.execute("SELECT current_database(), current_user, version();")
            db_name, user_name, version_text = cur.fetchone()

            print(f"Database: {db_name}")
            print(f"User: {user_name}")
            print(f"Version: {version_text.splitlines()[0]}")

            print("Creating app-state tables...")
            create_app_state_tables(cur)

            print("Cleaning prior local demo rows...")
            clean_prior_demo_rows(cur)

            print("Writing test triage status...")
            write_demo_triage_status(cur)

            print("Writing test note...")
            write_demo_note(cur)

            print("Writing activity audit row...")
            write_demo_activity(cur)

            validate_demo_row(cur)

            triage_rows, note_rows, ai_review_rows, activity_rows = fetch_row_counts(cur)

            print("Row counts:")
            print(f"  exception_triage_status: {triage_rows}")
            print(f"  exception_notes:         {note_rows}")
            print(f"  ai_rca_review_log:       {ai_review_rows}")
            print(f"  app_user_activity:       {activity_rows}")

        conn.commit()

    print("PostgreSQL writeback test completed successfully.")


if __name__ == "__main__":
    main()