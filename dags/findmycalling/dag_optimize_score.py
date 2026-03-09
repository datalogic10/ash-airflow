"""
FindMyCalling - Optimize & Score DAG

Replaces the optimize-jobs.yml and score-jobs.yml GitHub Actions workflows.
Runs optimize first, then score — proper dependency instead of 15-min delay hack.

Schedule: None (triggered by dag_fetch_jobs on success)
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import logging

logger = logging.getLogger(__name__)

APP_URL = os.environ.get("FMC_APP_URL", "https://findmycalling.vercel.app")
CRON_SECRET = os.environ.get("FMC_CRON_SECRET", "")

default_args = {
    "owner": "findmycalling",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=60),
    "execution_timeout": timedelta(seconds=300),
}

dag = DAG(
    "fmc_optimize_score",
    default_args=default_args,
    description="Optimize job descriptions then score them (triggered after fetch)",
    schedule_interval=None,  # Triggered by fmc_fetch_jobs
    catchup=False,
    max_active_runs=1,
    tags=["findmycalling", "optimize", "score", "llm"],
)


def call_api(endpoint: str, label: str, **context):
    """Hit a findmycalling API endpoint."""
    url = f"{APP_URL}{endpoint}"
    headers = {"Authorization": f"Bearer {CRON_SECRET}"}

    logger.info(f"[{label}] POST {url}")

    resp = requests.post(url, headers=headers, timeout=300)
    resp.raise_for_status()

    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
    logger.info(f"[{label}] Response ({resp.status_code}): {data}")
    return data


optimize = PythonOperator(
    task_id="optimize",
    python_callable=call_api,
    op_kwargs={"endpoint": "/api/jobs/optimize", "label": "optimize"},
    dag=dag,
)

score = PythonOperator(
    task_id="score",
    python_callable=call_api,
    op_kwargs={"endpoint": "/api/jobs/score", "label": "score"},
    dag=dag,
)

optimize >> score
