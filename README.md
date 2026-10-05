# Anime Data Platform

Initial architecture:

Jikan / AniList
      ↓
Apache Airflow
      ↓
Amazon S3 raw landing zone
      ↓
Databricks Bronze (Delta)

## Main folders

- `airflow/dags/` - orchestration only
- `airflow/include/clients/` - API clients
- `airflow/include/storage/` - S3 helpers
- `airflow/include/validation/` - source validation
- `databricks/bronze/` - Bronze ingestion logic
- `tests/` - tests
