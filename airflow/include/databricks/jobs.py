import os
import time
import logging
import requests


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def get_headers():
    if not DATABRICKS_HOST:
        raise ValueError("DATABRICKS_HOST is not set")

    if not DATABRICKS_TOKEN:
        raise ValueError("DATABRICKS_TOKEN is not set")

    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }


def trigger_databricks_job(
    job_id,
    ingestion_date=None,
):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"

    payload = {
        "job_id": job_id,
    }

    if ingestion_date:
        payload["notebook_params"] = {
            "ingestion_date": ingestion_date,
        }

    response = requests.post(
        url,
        headers=get_headers(),
        json=payload,
        timeout=60,
    )

    response.raise_for_status()

    run_id = response.json()["run_id"]

    logging.info(
        "Triggered Databricks job %s with run_id=%s",
        job_id,
        run_id,
    )

    return run_id


def get_databricks_run(run_id):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get"

    response = requests.get(
        url,
        headers=get_headers(),
        params={
            "run_id": run_id,
        },
        timeout=60,
    )

    response.raise_for_status()

    return response.json()


def wait_for_databricks_job(
    run_id,
    poll_interval=15,
    timeout=3600,
):
    start_time = time.time()

    while True:
        run = get_databricks_run(
            run_id
        )

        state = run.get(
            "state",
            {}
        )

        life_cycle_state = state.get(
            "life_cycle_state"
        )

        result_state = state.get(
            "result_state"
        )

        state_message = state.get(
            "state_message"
        )

        logging.info(
            "Databricks run %s lifecycle=%s result=%s",
            run_id,
            life_cycle_state,
            result_state,
        )

        if life_cycle_state == "TERMINATED":
            if result_state == "SUCCESS":
                return

            raise RuntimeError(
                f"Databricks run {run_id} failed. "
                f"result_state={result_state}, "
                f"message={state_message}"
            )

        if life_cycle_state in {
            "SKIPPED",
            "INTERNAL_ERROR",
        }:
            raise RuntimeError(
                f"Databricks run {run_id} failed. "
                f"life_cycle_state={life_cycle_state}, "
                f"message={state_message}"
            )

        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Databricks run {run_id} exceeded "
                f"{timeout} seconds"
            )

        time.sleep(
            poll_interval
        )


def run_databricks_job(
    job_id,
    ingestion_date=None,
):
    run_id = trigger_databricks_job(
        job_id=job_id,
        ingestion_date=ingestion_date,
    )

    wait_for_databricks_job(
        run_id=run_id,
    )

    return run_id