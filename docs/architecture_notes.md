# Architecture Notes

Databricks owns transformation, validation, lineage, and data-quality logic.

BigQuery is the default analytics target and source for Streamlit.

Snowflake powers the Business KPI Power BI dashboard.

Azure SQL powers the Data Reliability & Support Ops Power BI dashboard.

Azure Database for PostgreSQL stores Streamlit app-state writeback.

MongoDB Atlas preserves raw semi-structured event context.
