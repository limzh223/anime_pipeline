import logging
import tempfile
from pathlib import Path

import requests


IMDB_BASE_URL = "https://datasets.imdbws.com"

IMDB_DATASETS = {
    "title_basics": (
        f"{IMDB_BASE_URL}/title.basics.tsv.gz"
    ),
    "title_ratings": (
        f"{IMDB_BASE_URL}/title.ratings.tsv.gz"
    ),
}


def download_imdb_dataset(dataset_name):

    if dataset_name not in IMDB_DATASETS:
        raise ValueError(
            f"Unknown IMDb dataset: {dataset_name}"
        )

    url = IMDB_DATASETS[dataset_name]

    logging.info(
        "Downloading IMDb dataset: %s",
        dataset_name,
    )

    response = requests.get(
        url,
        stream=True,
        timeout=120,
    )

    response.raise_for_status()

    temp_dir = tempfile.mkdtemp()

    file_path = Path(
        temp_dir
    ) / f"{dataset_name}.tsv.gz"

    with open(file_path, "wb") as file:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):

            if chunk:
                file.write(chunk)

    if file_path.stat().st_size == 0:
        raise ValueError(
            f"IMDb {dataset_name} file is empty"
        )

    logging.info(
        "Downloaded IMDb dataset to %s",
        file_path,
    )

    return str(file_path)