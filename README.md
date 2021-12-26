# ETL project for Crypto data

### Simple project to manage crypto data using a data engineering pipeline

This project has basically the following structure:

- Extract data by parsing crypto websites
- Transform by parsing crawled data and making simple validation (removing null values)
- Saving data in a SQL database (SQLite)
- Orchestrated via Apache-Airflow