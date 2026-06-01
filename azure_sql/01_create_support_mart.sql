-- =============================================================================
-- MeterFlow IQ - Azure SQL Technical Support Mart Setup
-- =============================================================================
--
-- Purpose:
--   Creates and validates the Azure SQL support/reliability reporting foundation
--   for MeterFlow IQ.
--
-- Target:
--   Azure SQL Database: meterflow_iq_support
--   Default schema:     dbo
--
-- Intended use:
--   Databricks Gold outputs will be published into Azure SQL for the
--   Power BI Data Reliability & Support Ops dashboard.
--
-- Notes:
--   - No passwords, tokens, or secrets belong in this file.
--   - This version avoids GO and CREATE OR ALTER VIEW so it works better in
--     Azure Query Editor.
--   - The Databricks publisher may drop/recreate published tables during demo runs.
--
-- =============================================================================

SET NOCOUNT ON;

-- -----------------------------------------------------------------------------
-- Verify database context
-- -----------------------------------------------------------------------------

SELECT
    DB_NAME() AS current_database,
    SUSER_SNAME() AS login_name,
    @@SERVERNAME AS server_name,
    SYSDATETIMEOFFSET() AS checked_at;


-- -----------------------------------------------------------------------------
-- Optional metadata table for setup tracking
-- -----------------------------------------------------------------------------

IF OBJECT_ID('dbo._meterflow_iq_support_mart_setup', 'U') IS NULL
BEGIN
    CREATE TABLE dbo._meterflow_iq_support_mart_setup (
        setup_id int IDENTITY(1,1) NOT NULL CONSTRAINT pk_meterflow_iq_support_mart_setup PRIMARY KEY,
        setup_name varchar(200) NOT NULL,
        status varchar(50) NOT NULL,
        setup_notes varchar(1000) NULL,
        created_at_utc datetime2(3) NOT NULL CONSTRAINT df_meterflow_iq_support_mart_setup_created_at DEFAULT SYSUTCDATETIME()
    );
END;

INSERT INTO dbo._meterflow_iq_support_mart_setup (
    setup_name,
    status,
    setup_notes
)
VALUES (
    'azure_sql_support_mart_setup',
    'PASS',
    'Azure SQL support mart foundation initialized for MeterFlow IQ.'
);


-- -----------------------------------------------------------------------------
-- Empty placeholder tables
-- -----------------------------------------------------------------------------
-- These definitions document the expected technical support mart objects.
-- The Databricks publisher will load/recreate the actual data tables.
-- -----------------------------------------------------------------------------

IF OBJECT_ID('dbo.fact_pipeline_run', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_pipeline_run (
        pipeline_run_key varchar(200) NULL,
        run_id varchar(200) NULL,
        pipeline_name varchar(200) NULL,
        source_file varchar(500) NULL,
        started_at datetime2(3) NULL,
        completed_at datetime2(3) NULL,
        status varchar(100) NULL,
        rows_read bigint NULL,
        rows_accepted bigint NULL,
        rows_rejected bigint NULL,
        error_count bigint NULL,
        warning_count bigint NULL,
        error_message varchar(max) NULL,
        trigger_type varchar(100) NULL,
        environment varchar(100) NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;

IF OBJECT_ID('dbo.pipeline_health_summary', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.pipeline_health_summary (
        pipeline_name varchar(200) NULL,
        latest_status varchar(100) NULL,
        latest_run_started_at datetime2(3) NULL,
        latest_run_completed_at datetime2(3) NULL,
        latest_duration_minutes decimal(18,4) NULL,
        latest_rows_read bigint NULL,
        latest_rows_accepted bigint NULL,
        latest_rows_rejected bigint NULL,
        run_count bigint NULL,
        success_count bigint NULL,
        failed_count bigint NULL,
        partial_load_count bigint NULL,
        runs_with_errors_count bigint NULL,
        failure_rate decimal(18,6) NULL,
        overall_rejected_rate decimal(18,6) NULL,
        latest_error_message varchar(max) NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;

IF OBJECT_ID('dbo.fact_data_quality_exception', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_data_quality_exception (
        exception_id varchar(500) NULL,
        rule_id varchar(200) NULL,
        exception_type varchar(200) NULL,
        severity varchar(100) NULL,
        business_reason varchar(max) NULL,
        raw_reading_id varchar(200) NULL,
        meter_id varchar(100) NULL,
        facility_id varchar(100) NULL,
        facility_name varchar(300) NULL,
        region varchar(100) NULL,
        basin varchar(100) NULL,
        production_date date NULL,
        reading_timestamp datetime2(3) NULL,
        source_system varchar(200) NULL,
        polling_platform varchar(200) NULL,
        volume decimal(18,6) NULL,
        quality_code varchar(100) NULL,
        raw_status varchar(100) NULL,
        mongo_device_status varchar(100) NULL,
        mongo_communication_status varchar(100) NULL,
        mongo_signal_quality varchar(100) NULL,
        mongo_scenario_id varchar(100) NULL,
        primary_exception_type varchar(200) NULL,
        exception_status varchar(100) NULL,
        detected_at datetime2(3) NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;

IF OBJECT_ID('dbo.exception_summary_daily', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.exception_summary_daily (
        production_date date NULL,
        facility_id varchar(100) NULL,
        facility_name varchar(300) NULL,
        region varchar(100) NULL,
        basin varchar(100) NULL,
        exception_type varchar(200) NULL,
        severity varchar(100) NULL,
        exception_count bigint NULL,
        affected_reading_count bigint NULL,
        affected_meter_count bigint NULL,
        source_systems_text varchar(max) NULL,
        polling_platforms_text varchar(max) NULL,
        device_statuses_text varchar(max) NULL,
        communication_statuses_text varchar(max) NULL,
        signal_qualities_text varchar(max) NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;

IF OBJECT_ID('dbo.fact_source_to_target_reconciliation', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_source_to_target_reconciliation (
        stage_order int NULL,
        layer varchar(100) NULL,
        object_name varchar(300) NULL,
        row_count bigint NULL,
        previous_row_count bigint NULL,
        row_count_delta_from_previous bigint NULL,
        row_count_delta_pct_from_previous decimal(18,6) NULL,
        metric_type varchar(200) NULL,
        notes varchar(max) NULL,
        interpretation varchar(max) NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;

IF OBJECT_ID('dbo.fact_support_ticket', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.fact_support_ticket (
        ticket_id varchar(200) NULL,
        opened_datetime datetime2(3) NULL,
        closed_datetime datetime2(3) NULL,
        system_name varchar(200) NULL,
        issue_type varchar(200) NULL,
        severity varchar(100) NULL,
        facility_id varchar(100) NULL,
        meter_id varchar(100) NULL,
        reported_by_team varchar(200) NULL,
        ticket_status varchar(100) NULL,
        business_impact varchar(max) NULL,
        root_cause_category varchar(200) NULL,
        resolution_summary varchar(max) NULL,
        related_batch_id varchar(200) NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;

IF OBJECT_ID('dbo.quality_rule_summary', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.quality_rule_summary (
        rule_id varchar(200) NULL,
        rule_name varchar(300) NULL,
        layer varchar(100) NULL,
        field_name varchar(200) NULL,
        rule_type varchar(200) NULL,
        condition_description varchar(max) NULL,
        severity varchar(100) NULL,
        business_reason varchar(max) NULL,
        owner_team varchar(200) NULL,
        active_flag bit NULL,
        exception_count bigint NULL,
        affected_reading_count bigint NULL,
        affected_meter_count bigint NULL,
        _azure_sql_publish_run_id varchar(200) NULL,
        _azure_sql_published_at_utc datetime2(3) NULL
    );
END;


-- -----------------------------------------------------------------------------
-- Support/reliability views
-- -----------------------------------------------------------------------------
-- In SQL Server/Azure SQL, CREATE VIEW must be first in its batch.
-- To keep this script Azure Query Editor friendly, views are dropped first
-- and created with dynamic SQL.
-- -----------------------------------------------------------------------------

IF OBJECT_ID('dbo.vw_support_pipeline_health', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_support_pipeline_health;
END;

EXEC(N'
CREATE VIEW dbo.vw_support_pipeline_health AS
SELECT
    pipeline_name,
    latest_status,
    latest_run_started_at,
    latest_run_completed_at,
    latest_duration_minutes,
    latest_rows_read,
    latest_rows_accepted,
    latest_rows_rejected,
    run_count,
    success_count,
    failed_count,
    partial_load_count,
    runs_with_errors_count,
    failure_rate,
    overall_rejected_rate,
    latest_error_message,
    _azure_sql_publish_run_id,
    _azure_sql_published_at_utc
FROM dbo.pipeline_health_summary;
');


IF OBJECT_ID('dbo.vw_support_exception_backlog', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_support_exception_backlog;
END;

EXEC(N'
CREATE VIEW dbo.vw_support_exception_backlog AS
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
    detected_at,
    _azure_sql_publish_run_id,
    _azure_sql_published_at_utc
FROM dbo.fact_data_quality_exception;
');


IF OBJECT_ID('dbo.vw_support_exception_summary_daily', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_support_exception_summary_daily;
END;

EXEC(N'
CREATE VIEW dbo.vw_support_exception_summary_daily AS
SELECT
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
    _azure_sql_publish_run_id,
    _azure_sql_published_at_utc
FROM dbo.exception_summary_daily;
');


IF OBJECT_ID('dbo.vw_support_reconciliation_status', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_support_reconciliation_status;
END;

EXEC(N'
CREATE VIEW dbo.vw_support_reconciliation_status AS
SELECT
    stage_order,
    layer,
    object_name,
    row_count,
    previous_row_count,
    row_count_delta_from_previous,
    row_count_delta_pct_from_previous,
    metric_type,
    notes,
    interpretation,
    _azure_sql_publish_run_id,
    _azure_sql_published_at_utc
FROM dbo.fact_source_to_target_reconciliation;
');


IF OBJECT_ID('dbo.vw_support_ticket_rca', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_support_ticket_rca;
END;

EXEC(N'
CREATE VIEW dbo.vw_support_ticket_rca AS
SELECT
    ticket_id,
    opened_datetime,
    closed_datetime,
    system_name,
    issue_type,
    severity,
    facility_id,
    meter_id,
    reported_by_team,
    ticket_status,
    business_impact,
    root_cause_category,
    resolution_summary,
    related_batch_id,
    _azure_sql_publish_run_id,
    _azure_sql_published_at_utc
FROM dbo.fact_support_ticket;
');


IF OBJECT_ID('dbo.vw_support_data_reliability_overview', 'V') IS NOT NULL
BEGIN
    DROP VIEW dbo.vw_support_data_reliability_overview;
END;

EXEC(N'
CREATE VIEW dbo.vw_support_data_reliability_overview AS
SELECT
    ''pipeline_health'' AS metric_group,
    CAST(COUNT(*) AS bigint) AS metric_count,
    CAST(SUM(CASE WHEN latest_status = ''SUCCESS'' THEN 1 ELSE 0 END) AS bigint) AS passing_count,
    CAST(SUM(CASE WHEN latest_status <> ''SUCCESS'' THEN 1 ELSE 0 END) AS bigint) AS attention_count
FROM dbo.pipeline_health_summary

UNION ALL

SELECT
    ''data_quality_exceptions'' AS metric_group,
    CAST(COUNT(*) AS bigint) AS metric_count,
    CAST(SUM(CASE WHEN severity IN (''Low'', ''Medium'') THEN 1 ELSE 0 END) AS bigint) AS passing_count,
    CAST(SUM(CASE WHEN severity NOT IN (''Low'', ''Medium'') THEN 1 ELSE 0 END) AS bigint) AS attention_count
FROM dbo.fact_data_quality_exception

UNION ALL

SELECT
    ''source_to_target_reconciliation'' AS metric_group,
    CAST(COUNT(*) AS bigint) AS metric_count,
    CAST(SUM(CASE WHEN row_count IS NOT NULL THEN 1 ELSE 0 END) AS bigint) AS passing_count,
    CAST(SUM(CASE WHEN row_count IS NULL THEN 1 ELSE 0 END) AS bigint) AS attention_count
FROM dbo.fact_source_to_target_reconciliation;
');


-- -----------------------------------------------------------------------------
-- Verification queries
-- -----------------------------------------------------------------------------

SELECT
    'dbo._meterflow_iq_support_mart_setup' AS object_name,
    COUNT(*) AS row_count
FROM dbo._meterflow_iq_support_mart_setup;

SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'dbo'
  AND (
      TABLE_NAME LIKE 'fact_%'
      OR TABLE_NAME LIKE '%summary%'
      OR TABLE_NAME LIKE 'vw_support_%'
      OR TABLE_NAME LIKE '_meterflow%'
  )
ORDER BY
    TABLE_TYPE,
    TABLE_NAME;

SELECT
    'Azure SQL support mart setup complete.' AS status_message,
    DB_NAME() AS current_database,
    SUSER_SNAME() AS login_name,
    SYSUTCDATETIME() AS completed_at_utc;