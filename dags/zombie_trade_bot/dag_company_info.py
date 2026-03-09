"""
zombie-trade-bot - Company Info Update DAG (HTTP version)

Triggers monthly company info update on the zombie-trade-bot cloud instance.

Schedule: 1st of month at 8 PM ET (01:00 UTC on 2nd)
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
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
}

dag = DAG(
    "ztb_company_info_update",
    default_args=default_args,
    description="Trigger monthly company info update on zombie-trade-bot",
    schedule_interval="0 1 2 * *",
    catchup=False,
    max_active_runs=1,
    tags=["zombie-trade-bot", "company-info", "monthly"],
)


def trigger_company_info_update(**context):
    url = f"{APP_URL}/api/data/update"
    headers = {"Authorization": f"Bearer {API_SECRET}"}
    params = {"source": "yfinance_company_fundamentals", "days": 60, "incremental": True}

    logger.info(f"Triggering company info update: POST {url}")
    resp = requests.post(url, headers=headers, params=params, timeout=7200)
    resp.raise_for_status()

    data = resp.json()
    logger.info(f"Company info update response: {data}")

    if data.get("status") == "error":
        raise Exception(f"Company info update failed: {data.get('message')}")
    return data


PythonOperator(
    task_id="update_company_info",
    python_callable=trigger_company_info_update,
    dag=dag,
)
