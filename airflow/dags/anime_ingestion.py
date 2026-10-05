from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os

from include.clients.tenrai_client import fetch_tenrai_anime
from include.clients.anilist_client import fetch_anilist_anime
from include.clients.tmdb_client import fetch_tmdb_anime
from include.clients.imdb_client import download_imdb_dataset

from include.storage.s3 import (
    upload_json_to_s3,
    upload_file_to_s3,
)


S3_BUCKET = os.getenv("S3_BUCKET")


default_args = {
    "owner": "anime",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="anime_ingestion_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=[
        "anime",
        "ingestion",
        "minio",
    ],
) as dag:

    @task
    def tenrai_to_minio(ds=None):
        payload = fetch_tenrai_anime()

        payload["ingestion_date"] = ds

        key = (
            f"raw/tenrai/"
            f"ingestion_date={ds}/"
            f"anime.json"
        )

        upload_json_to_s3(
            payload=payload,
            bucket=S3_BUCKET,
            key=key,
        )

        return key


    @task
    def anilist_to_minio(ds=None):
        payload = fetch_anilist_anime()

        payload["ingestion_date"] = ds

        key = (
            f"raw/anilist/"
            f"ingestion_date={ds}/"
            f"anime.json"
        )

        upload_json_to_s3(
            payload=payload,
            bucket=S3_BUCKET,
            key=key,
        )

        return key


    @task
    def tmdb_to_minio(ds=None):
        payload = fetch_tmdb_anime()

        payload["ingestion_date"] = ds

        key = (
            f"raw/tmdb/"
            f"ingestion_date={ds}/"
            f"anime.json"
        )

        upload_json_to_s3(
            payload=payload,
            bucket=S3_BUCKET,
            key=key,
        )

        return key


    @task
    def imdb_to_minio(ds=None):
        datasets = [
            "title_basics",
            "title_ratings",
        ]

        uploaded_files = []

        for dataset in datasets:
            file_path = download_imdb_dataset(
                dataset
            )

            key = (
                f"raw/imdb/"
                f"ingestion_date={ds}/"
                f"{dataset}.tsv.gz"
            )

            upload_file_to_s3(
                file_path=file_path,
                bucket=S3_BUCKET,
                key=key,
            )

            uploaded_files.append(
                key
            )

        return uploaded_files


    tenrai_task = tenrai_to_minio()
    anilist_task = anilist_to_minio()
    tmdb_task = tmdb_to_minio()
    imdb_task = imdb_to_minio()