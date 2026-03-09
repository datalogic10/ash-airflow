"""
FindMyCalling - Fetch Jobs DAG

Replaces the fetch-jobs.yml GitHub Actions workflow.
Hits the findmycalling API to fetch jobs from 7 sources in parallel,
then triggers the optimize+score DAG on success.

Schedule: Twice daily at 8am and 8pm UTC
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import requests
import logging

logger = logging.getLogger(__name__)

APP_URL = os.environ.get("FMC_APP_URL", "https://findmycalling.vercel.app")
CRON_SECRET = os.environ.get("FMC_CRON_SECRET", "")

SOURCES = ["adzuna", "remotive", "arbeitnow", "jooble", "usajobs", "greenhouse", "lever"]

default_args = {
    "owner": "findmycalling",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "execution_timeout": timedelta(seconds=120),
}

dag = DAG(
    "fmc_fetch_jobs",
    default_args=default_args,
    description="Fetch jobs from all sources via findmycalling API",
    schedule_interval="0 8,20 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["findmycalling", "fetch", "jobs"],
)


def fetch_source(source: str, **context):
    """Fetch jobs for a single source by hitting the API."""
    url = f"{APP_URL}/api/jobs/fetch?source_type={source}"
    headers = {"Authorization": f"Bearer {CRON_SECRET}"}

    logger.info(f"Fetching jobs from {source}: POST {url}")

    resp = requests.post(url, headers=headers, timeout=120)
    resp.raise_for_status()

    data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
    logger.info(f"[{source}] Response ({resp.status_code}): {data}")
    return data


# Create one task per source — they run in parallel with SequentialExecutor
# (they'll actually run sequentially, but are logically independent)
fetch_tasks = []
for source in SOURCES:
    task = PythonOperator(
        task_id=f"fetch_{source}",
        python_callable=fetch_source,
        op_kwargs={"source": source},
        dag=dag,
    )
    fetch_tasks.append(task)

# After all fetches succeed, trigger optimize+score
trigger_optimize_score = TriggerDagRunOperator(
    task_id="trigger_optimize_score",
    trigger_dag_id="fmc_optimize_score",
    dag=dag,
)

fetch_tasks >> trigger_optimize_score
