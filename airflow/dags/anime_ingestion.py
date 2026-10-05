from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from include.clients.tenrai_client import (
    fetch_tenrai_anime,
)

from include.clients.anilist_client import (
    fetch_anilist_anime,
)

from include.clients.tmdb_client import (
    fetch_tmdb_anime,
)

from include.clients.imdb_client import (
    download_imdb_dataset,
)

from include.storage.databricks import (
    upload_json_to_databricks,
    upload_file_to_databricks,
)


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
        "databricks",
    ],
) as dag:

    @task
    def tenrai_to_databricks(ds=None):

        payload = fetch_tenrai_anime()

        payload["ingestion_date"] = ds

        path = (
            f"tenrai/"
            f"ingestion_date={ds}/"
            f"anime.json"
        )

        return upload_json_to_databricks(
            payload=payload,
            path=path,
        )


    @task
    def anilist_to_databricks(ds=None):

        payload = fetch_anilist_anime()

        payload["ingestion_date"] = ds

        path = (
            f"anilist/"
            f"ingestion_date={ds}/"
            f"anime.json"
        )

        return upload_json_to_databricks(
            payload=payload,
            path=path,
        )


    @task
    def tmdb_to_databricks(ds=None):

        payload = fetch_tmdb_anime()

        payload["ingestion_date"] = ds

        path = (
            f"tmdb/"
            f"ingestion_date={ds}/"
            f"anime.json"
        )

        return upload_json_to_databricks(
            payload=payload,
            path=path,
        )


    @task
    def imdb_to_databricks(ds=None):

        datasets = [
            "title_basics",
            "title_ratings",
        ]

        uploaded_files = []

        for dataset in datasets:

            file_path = download_imdb_dataset(
                dataset
            )

            path = (
                f"imdb/"
                f"ingestion_date={ds}/"
                f"{dataset}.tsv.gz"
            )

            uploaded_path = (
                upload_file_to_databricks(
                    file_path=file_path,
                    path=path,
                )
            )

            uploaded_files.append(
                uploaded_path
            )

        return uploaded_files


    tenrai_task = tenrai_to_databricks()
    anilist_task = anilist_to_databricks()
    tmdb_task = tmdb_to_databricks()
    imdb_task = imdb_to_databricks()