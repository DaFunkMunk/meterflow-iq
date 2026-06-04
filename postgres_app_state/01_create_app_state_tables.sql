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

CREATE TABLE IF NOT EXISTS exception_notes (
    note_id BIGSERIAL PRIMARY KEY,
    exception_group_key TEXT NOT NULL,
    exception_id TEXT NULL,
    note_text TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT 'streamlit_user',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS app_user_activity (
    activity_id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL DEFAULT 'streamlit_user',
    action_type TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NULL,
    activity_detail JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exception_triage_status_group_key
    ON exception_triage_status (exception_group_key);

CREATE INDEX IF NOT EXISTS idx_exception_notes_group_key
    ON exception_notes (exception_group_key);

CREATE INDEX IF NOT EXISTS idx_ai_rca_review_log_group_key
    ON ai_rca_review_log (exception_group_key);

CREATE INDEX IF NOT EXISTS idx_app_user_activity_object
    ON app_user_activity (object_type, object_id);