# MeterFlow IQ

MeterFlow IQ is an operations data quality command center portfolio project.

It simulates structured meter/source data and semi-structured MongoDB Atlas event payloads, processes them through a Databricks Bronze/Silver/Gold pipeline, and publishes curated outputs to dedicated serving layers:

- BigQuery for the Streamlit investigation command center
- Snowflake for the Power BI Business KPI Dashboard
- Azure SQL for the Power BI Data Reliability & Support Ops Dashboard
- Azure Database for PostgreSQL for Streamlit app-state writeback

The project focuses on data quality, source-to-target reconciliation, pipeline observability, operational support workflows, and trusted reporting.
