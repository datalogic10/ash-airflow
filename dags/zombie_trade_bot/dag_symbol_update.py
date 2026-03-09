"""
zombie-trade-bot - Symbol Update DAG (HTTP version)

Triggers NASDAQ symbol list update on the zombie-trade-bot cloud instance.

Schedule: Monday 6 AM ET (11:00 UTC)
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
import logging

logger = logging.getLogger(__name__)

APP_URL = os.environ.get("ZTB_APP_URL", "")
API_SECRET = os.environ.get("ZTB_API_SECRET", "")

default_args = {
    "owner": "zombie-trade-bot",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ztb_symbol_update",
    default_args=default_args,
    description="Trigger NASDAQ symbol update on zombie-trade-bot",
    schedule_interval="0 11 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["zombie-trade-bot", "symbols", "weekly"],
)


def trigger_symbol_update(**context):
    url = f"{APP_URL}/api/symbols/update"
    headers = {"Authorization": f"Bearer {API_SECRET}"}

    logger.info(f"Triggering symbol update: POST {url}")
    resp = requests.post(url, headers=headers, timeout=600)
    resp.raise_for_status()

    data = resp.json()
    logger.info(f"Symbol update response: {data}")

    if data.get("status") == "error":
        raise Exception(f"Symbol update failed: {data.get('message')}")
    return data


PythonOperator(
    task_id="update_symbols",
    python_callable=trigger_symbol_update,
    dag=dag,
)
