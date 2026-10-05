<<<<<<< HEAD
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
=======
# anime_pipeline
>>>>>>> 7e74dacf048f1da6f78b50eee04e955c43d461bc
