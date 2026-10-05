import os
import json
import logging
import requests


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

CATALOG = os.getenv("DATABRICKS_CATALOG")
SCHEMA = os.getenv("DATABRICKS_SCHEMA")
VOLUME = os.getenv("DATABRICKS_VOLUME")


def _headers():
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream",
    }


def upload_bytes_to_databricks(
    data,
    path,
):
    volume_path = (
        f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{path}"
    )

    url = (
        f"{DATABRICKS_HOST}"
        f"/api/2.0/fs/files"
        f"{volume_path}"
    )

    response = requests.put(
        url,
        headers=_headers(),
        params={"overwrite": "true"},
        data=data,
        timeout=120,
    )

    response.raise_for_status()

    logging.info(
        "Uploaded to Databricks: %s",
        volume_path,
    )

    return volume_path


def upload_json_to_databricks(
    payload,
    path,
):
    data = json.dumps(
        payload,
        ensure_ascii=False,
    ).encode("utf-8")

    return upload_bytes_to_databricks(
        data=data,
        path=path,
    )


def upload_file_to_databricks(
    file_path,
    path,
):
    with open(file_path, "rb") as file:
        data = file.read()

    return upload_bytes_to_databricks(
        data=data,
        path=path,
    )