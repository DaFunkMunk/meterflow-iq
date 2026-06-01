-- Snowflake business reporting schema placeholder.
-- Create database/schema/tables for Business KPI Power BI dashboard.
-- =============================================================================
-- MeterFlow IQ - Snowflake Business Reporting Setup
-- =============================================================================
--
-- Purpose:
--   Creates the Snowflake business reporting foundation for MeterFlow IQ.
--
-- This script creates:
--   - Resource monitor for basic cost guardrails
--   - X-Small project warehouse with 60-second auto-suspend
--   - METERFLOW_IQ database
--   - CURATED schema
--   - METERFLOW_IQ_ROLE project role
--   - Grants needed for Databricks publisher and Power BI business reporting
--
-- Notes:
--   - Run as ACCOUNTADMIN.
--   - No passwords, tokens, or secrets belong in this file.
--   - The Databricks publisher will write curated Gold outputs into:
--       METERFLOW_IQ.CURATED
--
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Admin context
-- -----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;


-- -----------------------------------------------------------------------------
-- Cost guardrail
-- -----------------------------------------------------------------------------
-- Conservative monitor for the portfolio demo.
-- If this script is reused in another account, adjust the quota as needed.
-- -----------------------------------------------------------------------------

CREATE RESOURCE MONITOR IF NOT EXISTS METERFLOW_IQ_RESOURCE_MONITOR
  WITH CREDIT_QUOTA = 10
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 50 PERCENT DO NOTIFY
    ON 80 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;


-- -----------------------------------------------------------------------------
-- Warehouse
-- -----------------------------------------------------------------------------
-- X-Small warehouse with short auto-suspend for cost control.
-- -----------------------------------------------------------------------------

CREATE WAREHOUSE IF NOT EXISTS METERFLOW_IQ_WH
  WITH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'MeterFlow IQ portfolio demo warehouse. X-Small with 60-second auto-suspend.';

ALTER WAREHOUSE METERFLOW_IQ_WH
  SET RESOURCE_MONITOR = METERFLOW_IQ_RESOURCE_MONITOR;


-- -----------------------------------------------------------------------------
-- Database and schema
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS METERFLOW_IQ
  COMMENT = 'MeterFlow IQ portfolio project database.';

CREATE SCHEMA IF NOT EXISTS METERFLOW_IQ.CURATED
  COMMENT = 'Business KPI curated schema published from Databricks Gold outputs.';


-- -----------------------------------------------------------------------------
-- Project role
-- -----------------------------------------------------------------------------
-- This role is intended for:
--   - Databricks Snowflake publisher
--   - business reporting validation
--   - Power BI business KPI source access
-- -----------------------------------------------------------------------------

CREATE ROLE IF NOT EXISTS METERFLOW_IQ_ROLE
  COMMENT = 'Role for MeterFlow IQ publishing and business reporting objects.';


-- -----------------------------------------------------------------------------
-- Grant project role to the current setup user
-- -----------------------------------------------------------------------------
-- This avoids hard-coding a personal username in the repo.
-- When run by a different Snowflake user, the role is granted to that user.
-- -----------------------------------------------------------------------------

SET METERFLOW_IQ_SETUP_USER = CURRENT_USER();

GRANT ROLE METERFLOW_IQ_ROLE
  TO USER IDENTIFIER($METERFLOW_IQ_SETUP_USER);


-- -----------------------------------------------------------------------------
-- Warehouse grants
-- -----------------------------------------------------------------------------

GRANT USAGE
  ON WAREHOUSE METERFLOW_IQ_WH
  TO ROLE METERFLOW_IQ_ROLE;


-- -----------------------------------------------------------------------------
-- Database and schema grants
-- -----------------------------------------------------------------------------

GRANT USAGE
  ON DATABASE METERFLOW_IQ
  TO ROLE METERFLOW_IQ_ROLE;

GRANT USAGE
  ON SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;

GRANT CREATE TABLE
  ON SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;

GRANT CREATE VIEW
  ON SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;

GRANT CREATE STAGE
  ON SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;

GRANT CREATE FILE FORMAT
  ON SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;


-- -----------------------------------------------------------------------------
-- Future object grants
-- -----------------------------------------------------------------------------
-- These allow future tables/views created by the publisher to be queried later
-- without manually re-granting each object.
-- -----------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON FUTURE TABLES IN SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;

GRANT SELECT
  ON FUTURE VIEWS IN SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;


-- -----------------------------------------------------------------------------
-- Existing object grants
-- -----------------------------------------------------------------------------
-- Safe to run even if the schema is currently empty.
-- -----------------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;

GRANT SELECT
  ON ALL VIEWS IN SCHEMA METERFLOW_IQ.CURATED
  TO ROLE METERFLOW_IQ_ROLE;


-- -----------------------------------------------------------------------------
-- Verification queries
-- -----------------------------------------------------------------------------

USE ROLE METERFLOW_IQ_ROLE;
USE WAREHOUSE METERFLOW_IQ_WH;
USE DATABASE METERFLOW_IQ;
USE SCHEMA CURATED;

SELECT
  CURRENT_ROLE() AS current_role,
  CURRENT_WAREHOUSE() AS current_warehouse,
  CURRENT_DATABASE() AS current_database,
  CURRENT_SCHEMA() AS current_schema;

SHOW WAREHOUSES LIKE 'METERFLOW_IQ_WH';

SHOW DATABASES LIKE 'METERFLOW_IQ';

SHOW SCHEMAS IN DATABASE METERFLOW_IQ;


-- -----------------------------------------------------------------------------
-- Optional smoke test
-- -----------------------------------------------------------------------------
-- Uncomment and run manually if validating the role from a Snowflake worksheet.
-- Do not leave this table in the final database.
-- -----------------------------------------------------------------------------

/*
CREATE OR REPLACE TABLE METERFLOW_IQ.CURATED._SETUP_ROLE_SMOKE_TEST (
  test_name STRING,
  status STRING,
  created_at TIMESTAMP_NTZ
);

INSERT INTO METERFLOW_IQ.CURATED._SETUP_ROLE_SMOKE_TEST
SELECT
  'snowflake_role_smoke_test',
  'PASS',
  CURRENT_TIMESTAMP();

SELECT *
FROM METERFLOW_IQ.CURATED._SETUP_ROLE_SMOKE_TEST;

DROP TABLE METERFLOW_IQ.CURATED._SETUP_ROLE_SMOKE_TEST;
*/


-- -----------------------------------------------------------------------------
-- Cost-control reminder
-- -----------------------------------------------------------------------------
-- The warehouse auto-suspends after 60 seconds.
-- If you manually suspend it and Snowflake says it cannot be suspended,
-- it is probably already suspended.
-- -----------------------------------------------------------------------------

-- USE ROLE ACCOUNTADMIN;
-- ALTER WAREHOUSE METERFLOW_IQ_WH SUSPEND;