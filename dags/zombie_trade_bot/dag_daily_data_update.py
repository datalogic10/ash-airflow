"""
zombie-trade-bot - Daily Data Update DAG (HTTP version)

Triggers data updates on the zombie-trade-bot cloud instance.
Each source runs as a separate task, all trigger ML features on completion.

Schedule: 4:30 PM ET (21:30 UTC) on weekdays, after market close
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import requests
import logging

logger = logging.getLogger(__name__)

APP_URL = os.environ.get("ZTB_APP_URL", "")
API_SECRET = os.environ.get("ZTB_API_SECRET", "")

SOURCES = [
    ("yfinance_daily", "daily"),
    ("yfinance_earnings", "earnings"),
    ("yfinance_company_fundamentals", "fundamentals"),
    ("yfinance_quarterly_income", "quarterly_income"),
    ("yfinance_quarterly_balance", "quarterly_balance"),
    ("yfinance_quarterly_cashflow", "quarterly_cashflow"),
]

default_args = {
    "owner": "zombie-trade-bot",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    "ztb_daily_data_update",
    default_args=default_args,
    description="Trigger daily data updates on zombie-trade-bot",
    schedule_interval="30 21 * * 1-5",
    catchup=False,
    max_active_runs=1,
    tags=["zombie-trade-bot", "data", "daily"],
)


def trigger_data_update(source: str, **context):
    """POST to zombie-trade-bot API to trigger a data source update."""
    url = f"{APP_URL}/api/data/update"
    headers = {"Authorization": f"Bearer {API_SECRET}"}
    params = {"source": source, "days": 7, "incremental": True}

    logger.info(f"Triggering {source} update: POST {url}")

    resp = requests.post(url, headers=headers, params=params, timeout=3600)
    resp.raise_for_status()

    data = resp.json()
    logger.info(f"[{source}] Response: {data}")

    if data.get("status") == "error":
        raise Exception(f"{source} update failed: {data.get('message')}")

    return data


for source_name, task_suffix in SOURCES:
    PythonOperator(
        task_id=f"update_{task_suffix}",
        python_callable=trigger_data_update,
        op_kwargs={"source": source_name},
        dag=dag,
    )

trigger_ml = TriggerDagRunOperator(
    task_id="trigger_ml_features",
    trigger_dag_id="ztb_ml_feature_precompute",
    dag=dag,
)

for source_name, task_suffix in SOURCES:
    dag.get_task(f"update_{task_suffix}") >> trigger_ml
