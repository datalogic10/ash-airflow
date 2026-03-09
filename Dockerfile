FROM apache/airflow:2.8.1-python3.11

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY dags/ /opt/airflow/dags/
COPY entrypoint.sh /opt/airflow/entrypoint.sh

CMD ["bash", "/opt/airflow/entrypoint.sh"]
