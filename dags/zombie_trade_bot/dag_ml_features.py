"""
zombie-trade-bot - ML Feature Pre-computation DAG (HTTP version)

Triggers ML feature pre-computation on the zombie-trade-bot cloud instance.

Schedule: None (triggered by ztb_daily_data_update DAG)
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

dag = DAG(
    "ztb_ml_feature_precompute",
    default_args=default_args,
    description="Trigger ML feature pre-computation on zombie-trade-bot",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["zombie-trade-bot", "ml", "features"],
)


def trigger_feature_precompute(**context):
    url = f"{APP_URL}/api/features/compute"
    headers = {"Authorization": f"Bearer {API_SECRET}"}
    params = {"days": 30}

    logger.info(f"Triggering feature compute: POST {url}")
    resp = requests.post(url, headers=headers, params=params, timeout=3600)
    resp.raise_for_status()

    data = resp.json()
    logger.info(f"Feature precompute response: {data}")

    if data.get("status") == "error":
        raise Exception(f"Feature precompute failed: {data.get('message')}")
    return data


PythonOperator(
    task_id="precompute_features",
    python_callable=trigger_feature_precompute,
    dag=dag,
)
