"""
FindMyCalling - Generate Stories DAG

Replaces the generate-stories.yml GitHub Actions workflow.
Weekly story regeneration via API call.

Schedule: Sunday midnight UTC
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
    "fmc_generate_stories",
    default_args=default_args,
    description="Weekly story regeneration via findmycalling API",
    schedule_interval="0 0 * * 0",  # Sunday midnight UTC
    catchup=False,
    max_active_runs=1,
    tags=["findmycalling", "stories", "weekly"],
)


def generate_stories(**context):
    """Hit the story generation endpoint."""
    url = f"{APP_URL}/api/story/generate"
    headers = {"Authorization": f"Bearer {CRON_SECRET}"}

    logger.info(f"Generating stories: POST {url}")

    resp = requests.post(url, headers=headers, timeout=300)
    resp.raise_for_status()

    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
    logger.info(f"Story generation response ({resp.status_code}): {data}")
    return data


PythonOperator(
    task_id="generate_stories",
    python_callable=generate_stories,
    dag=dag,
)
