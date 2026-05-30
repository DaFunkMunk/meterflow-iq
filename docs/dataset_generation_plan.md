# Dataset Generation Plan

The first generated dataset package will include:

- facility_master.csv
- meter_master.csv
- raw_polling_readings.csv
- flowcal_measurement_extract.csv
- nominations_daily.csv
- support_tickets.csv
- pipeline_run_log.csv
- dq_rules_reference.csv
- known_issue_scenarios.csv

The CSVs are generated first. Bronze, Silver, Gold, BigQuery, Snowflake, Azure SQL, PostgreSQL app-state rows, Streamlit outputs, and Power BI outputs are created later by scripts/pipelines.
