import json
import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_CONN_ID = "aws_default"


def upload_json_to_s3(payload, bucket, key):
    if not bucket:
        raise ValueError(
            "S3_BUCKET environment variable is not set"
        )

    s3 = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    s3.load_string(
        string_data=json.dumps(
            payload,
            ensure_ascii=False,
        ),
        key=key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(
        "Uploaded JSON to s3://%s/%s",
        bucket,
        key,
    )


def upload_file_to_s3(file_path, bucket, key):
    if not bucket:
        raise ValueError(
            "S3_BUCKET environment variable is not set"
        )

    s3 = S3Hook(
        aws_conn_id=AWS_CONN_ID
    )

    s3.load_file(
        filename=file_path,
        key=key,
        bucket_name=bucket,
        replace=True,
    )

    logging.info(
        "Uploaded file to s3://%s/%s",
        bucket,
        key,
    )