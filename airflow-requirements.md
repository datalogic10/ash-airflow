# Centralized Airflow Orchestration — Requirements Doc

## Goal

Replace scattered scheduling (GitHub Actions for findmycalling, local Docker for zombie-trade-bot) with one centralized Airflow instance that orchestrates both projects. Deploy to Railway or Render free tier.

## Why Airflow (vs GitHub Actions + local Docker)

| | GitHub Actions | Local Docker Airflow | Centralized Airflow |
|---|---|---|---|
| Visibility | Per-repo, no cross-project view | Local only, loses state on restart | Single UI for all jobs |
| Dependencies | Can't express "run B after A finishes" | Works, but only within zombie-trade-bot | Cross-project dependencies possible |
| Monitoring | Check each repo separately | Local Airflow UI | One dashboard, alerting |
| Cost | Free (2000 min/month) | Free but must be running | Railway free tier ($5/mo if over) |
| dbt integration | Hacky (curl to API) | Not set up | Native BashOperator or dbt-core |

## Current State

### findmycalling (GitHub Actions)

4 workflows, all hitting Next.js API routes via `curl`:

| Workflow | Schedule | What it does |
|---|---|---|
| `fetch-jobs.yml` | 8am/8pm UTC | Hits `/api/jobs/fetch?source_type=X` for each of 7 sources |
| `optimize-jobs.yml` | 8:15am/8:15pm UTC | Hits `/api/jobs/optimize` (LLM JD optimization) |
| `score-jobs.yml` | 8:30am/8:30pm UTC | Hits `/api/jobs/score` (LLM scoring) |
| `generate-stories.yml` | Sunday midnight UTC | Hits `/api/story/generate` (weekly story regeneration) |

**Pattern**: fetch -> optimize (15 min later) -> score (30 min later). These are time-delayed because GitHub Actions can't express "run after previous completes." Airflow can.

### zombie-trade-bot (Local Docker Airflow)

Already has Airflow via `docker-compose.airflow-minimal.yml`:
- Apache Airflow 2.8.1, Python 3.11, SequentialExecutor, SQLite backend
- 4 DAGs: `daily_data_update`, `symbol_update_dag`, `company_info_update`, `ml_feature_precompute_dag`
- Volumes mount `./dags`, `./src`, `./scripts`, `./data` into the container
- Runs locally — loses state when laptop sleeps

## Architecture

```
Railway / Render (cloud)
└── Docker container
    └── Airflow standalone (webserver + scheduler)
        ├── DAGs
        │   ├── findmycalling/
        │   │   ├── dag_fetch_jobs.py        (replaces fetch-jobs.yml)
        │   │   ├── dag_optimize_score.py    (replaces optimize + score workflows)
        │   │   ├── dag_generate_stories.py  (replaces generate-stories.yml)
        │   │   └── dag_dbt_build.py         (new: runs dbt build)
        │   └── zombie_trade_bot/
        │       ├── dag_daily_data_update.py (existing, adapted)
        │       ├── dag_symbol_update.py     (existing, adapted)
        │       ├── dag_company_info.py      (existing, adapted)
        │       └── dag_ml_features.py       (existing, adapted)
        ├── Airflow metadata DB (SQLite or Postgres)
        └── Logs
```

### Key Difference from Current Setup

The zombie-trade-bot DAGs run Python code that connects to its PostgreSQL database directly (yfinance download, pandas transforms, SQL inserts). These need the `src/` Python modules mounted or installed.

The findmycalling DAGs are simpler — they just hit HTTP endpoints (the Next.js API routes on Vercel do the actual work). No Python dependencies needed beyond `requests`.

The dbt DAG runs `dbt build` which needs `dbt-postgres` installed and Supabase credentials.

## Project Structure

New standalone repo: `airflow-orchestrator/` (or add to an existing monorepo)

```
airflow-orchestrator/
├── Dockerfile
├── docker-compose.yml          # Local dev
├── requirements.txt            # Python deps (apache-airflow, dbt-postgres, yfinance, etc.)
├── dags/
│   ├── findmycalling/
│   │   ├── dag_fetch_jobs.py
│   │   ├── dag_optimize_score.py
│   │   ├── dag_generate_stories.py
│   │   └── dag_dbt_build.py
│   └── zombie_trade_bot/
│       ├── dag_daily_data_update.py
│       ├── dag_symbol_update.py
│       ├── dag_company_info.py
│       └── dag_ml_features.py
├── plugins/                    # Custom Airflow operators if needed
├── include/
│   └── dbt/                    # dbt project files (copy or git-sync from findmycalling/dbt/)
├── .env.example
└── README.md
```

## DAG Specifications

### findmycalling DAGs

#### dag_fetch_jobs.py
- **Schedule**: `0 8,20 * * *` (twice daily)
- **Tasks**: 7 parallel tasks (one per source: adzuna, remotive, arbeitnow, jooble, usajobs, greenhouse, lever)
- **Each task**: HTTP POST to `{APP_URL}/api/jobs/fetch?source_type={source}`
- **Headers**: `Authorization: Bearer {CRON_SECRET}`
- **Timeout**: 120s per source
- **Retries**: 2, delay 60s
- **On failure**: Log which sources failed, continue others

#### dag_optimize_score.py
- **Schedule**: None (triggered by dag_fetch_jobs on success via TriggerDagRunOperator)
- **Tasks**:
  1. `optimize` — POST to `{APP_URL}/api/jobs/optimize`
  2. `score` — POST to `{APP_URL}/api/jobs/score` (runs AFTER optimize completes)
- **Why combined**: These are sequential dependencies. Airflow handles this natively instead of the current 15-min delay hack.
- **Timeout**: 300s each (LLM processing is slow)
- **Retries**: 1

#### dag_generate_stories.py
- **Schedule**: `0 0 * * 0` (weekly, Sunday midnight UTC)
- **Tasks**: POST to `{APP_URL}/api/story/generate`
- **Timeout**: 300s
- **Retries**: 1

#### dag_dbt_build.py
- **Schedule**: `30 9,21 * * *` (90 min after fetch, giving optimize+score time to finish)
- **Alternative**: Triggered by dag_optimize_score completion
- **Tasks**: BashOperator running `dbt build --profiles-dir /opt/airflow/include/dbt --project-dir /opt/airflow/include/dbt`
- **Env vars needed**: `DBT_PASSWORD` (Supabase database password)
- **Timeout**: 300s
- **Retries**: 1

### zombie-trade-bot DAGs

Adapt existing DAGs from `zombie-trade-bot/dags/`. Main changes:
- Update import paths (the `src/` package needs to be pip-installed or mounted)
- Use Airflow Variables/Connections instead of hardcoded config
- Add proper error handling for cloud environment (no local filesystem for data/)

#### dag_daily_data_update.py
- **Schedule**: `30 21 * * 1-5` (4:30 PM ET, weekdays only)
- **Tasks**: TaskGroup with sequential market data, earnings, financials updates
- **Dependencies**: PostgreSQL connection to zombie-trade-bot database
- **Python packages**: yfinance, pandas, psycopg2, sqlalchemy

#### dag_symbol_update.py
- **Schedule**: `0 11 * * 1` (weekly Monday 6 AM ET)
- **Tasks**: Update master_symbol_list from NASDAQ

#### dag_company_info.py
- **Schedule**: `0 12 1 * *` (monthly, 1st of month)
- **Tasks**: Update company info from SEC EDGAR / SimFin

#### dag_ml_features.py
- **Schedule**: `0 23 * * 1-5` (6 PM ET, after data update)
- **Triggered by**: dag_daily_data_update completion (preferred) or scheduled
- **Tasks**: Compute ML features, write to feature store

## Environment Variables

```bash
# Airflow core
AIRFLOW__CORE__EXECUTOR=SequentialExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__WEBSERVER__SECRET_KEY=<random-string>

# findmycalling
FMC_APP_URL=https://findmycalling.vercel.app
FMC_CRON_SECRET=<from-vercel-env>
DBT_HOST=aws-1-us-east-2.pooler.supabase.com
DBT_USER=postgres.yjmsfowvteuyhhlldtng
DBT_PASSWORD=<supabase-db-password>

# zombie-trade-bot
ZTB_POSTGRES_HOST=<postgres-host>
ZTB_POSTGRES_PORT=5432
ZTB_POSTGRES_USER=<user>
ZTB_POSTGRES_PASSWORD=<password>
ZTB_POSTGRES_DB=<dbname>
```

## Dockerfile

```dockerfile
FROM apache/airflow:2.8.1-python3.11

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy DAGs
COPY dags/ /opt/airflow/dags/

# Copy dbt project for findmycalling
COPY include/dbt/ /opt/airflow/include/dbt/

# Copy zombie-trade-bot source (needed by its DAGs)
# Option A: pip install from git
# Option B: COPY src/ /opt/airflow/zombie_src/ and add to PYTHONPATH
```

### requirements.txt
```
dbt-postgres==1.10.0
yfinance==0.2.65
pandas>=1.5.3,<2.0
psycopg2-binary
sqlalchemy<2.0
requests
```

## Deployment Options

### Railway (recommended)
- Free tier: $5/month credit, enough for a lightweight Airflow
- Deploy via Dockerfile
- Persistent volume for SQLite metadata + logs
- Set env vars in Railway dashboard
- Public URL for Airflow webserver (password-protect it)

### Render
- Free tier: spins down after 15 min of inactivity (DAGs won't run on schedule)
- Would need Render's cron jobs feature instead (limited)
- Not recommended unless paying for always-on

### Fly.io
- Free tier: 3 shared VMs
- Better than Render for always-on
- `fly launch` with Dockerfile

## Migration Plan

### Step 1: Set up locally
1. Create `airflow-orchestrator/` repo
2. Copy zombie-trade-bot DAGs, adapt imports
3. Write findmycalling DAGs (HTTP-based, simple)
4. Write dbt DAG
5. `docker compose up` — verify all DAGs appear and run

### Step 2: Deploy to Railway
1. Push repo to GitHub
2. Connect Railway to GitHub repo
3. Set env vars
4. Verify DAGs run on schedule
5. Monitor for a week

### Step 3: Decommission old schedulers
1. Disable GitHub Actions workflows in findmycalling (don't delete — keep as backup)
2. Stop local Docker Airflow for zombie-trade-bot
3. Update README.md in both projects pointing to centralized Airflow

## Gotchas

- **Railway free tier sleep**: Railway doesn't spin down paid apps, but the $5 credit gets consumed by uptime hours. Airflow with SQLite + SequentialExecutor is lightweight enough (~256MB RAM).
- **zombie-trade-bot data directory**: The existing DAGs write to `./data/` and `./ml_data/`. In cloud, these need to be either: (a) stored in the database instead, or (b) use a persistent volume on Railway.
- **dbt packages**: Run `dbt deps` during Docker build so packages are cached in the image.
- **Secrets rotation**: If Supabase or API keys change, update Railway env vars (single place vs GitHub + local).
- **Timezone**: zombie-trade-bot uses America/New_York. Keep that in its DAGs but use UTC for findmycalling DAGs.
- **Monitoring**: Use Airflow's built-in email on failure. Consider Slack webhook for alerts (Airflow has a SlackOperator).

## Learning Value (for interviews)

This project demonstrates several production ML/data engineering patterns:
- **Orchestration**: Airflow is the most-asked-about tool in data engineering interviews
- **DAG design**: Dependencies, retries, alerting, idempotency
- **Infrastructure as code**: Dockerfile, docker-compose, environment management
- **Multi-project orchestration**: Real-world pattern for platform teams
- **dbt + Airflow**: The canonical modern data stack (dbt for transformation, Airflow for orchestration)
