#!/bin/bash
set -e

airflow db init

airflow users create \
  --username admin \
  --password "${AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com 2>/dev/null || true

# Start webserver in background, scheduler in foreground
# Use exec for scheduler so it gets signals properly
airflow webserver --port 8080 &
WEBSERVER_PID=$!

# Trap signals to kill both processes
trap "kill $WEBSERVER_PID; exit 0" SIGTERM SIGINT

airflow scheduler &
SCHEDULER_PID=$!

# Wait for either process to exit
wait -n $WEBSERVER_PID $SCHEDULER_PID
