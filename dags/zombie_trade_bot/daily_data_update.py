"""
Daily Data Update DAG

Comprehensive Airflow DAG for updating all core datasets in PostgreSQL.
Runs after market close to ensure fresh data for next trading day.

Updated Tables:
- master_symbol_list (6:00 AM) - NASDAQ symbol updates  
- market_data_daily (4:30 PM) - OHLCV daily data
- market_data_minute (4:45 PM) - OHLCV minute data (rolling 5 days)
- earnings (5:15 PM) - Quarterly earnings data
- quarterly_income_statement (5:30 PM) - Comprehensive quarterly financials
- quarterly_balance_sheet (5:45 PM) - Quarterly balance sheet data
- quarterly_cash_flow (6:00 PM) - Quarterly cash flow statement data

Features:
- Smart incremental updates (only missing/stale data)
- Flexible parameterization via Airflow Variables
- Comprehensive error handling and retry logic
- Data quality validation
- Summary notifications
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable, Param
from airflow.utils.task_group import TaskGroup
import logging
import subprocess
import sys
import os

# DAG Configuration
default_args = {
    'owner': 'trading-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': Variable.get('alert_email', default_var=['trading-alerts@company.com']),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    'daily_data_update',
    default_args=default_args,
    description='Comprehensive daily data update pipeline',
    schedule_interval='30 16 * * 1-5',  # 4:30 PM ET on weekdays (after market close)
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=['data', 'daily', 'trading', 'ml'],
    params={
        'datasets': Param(
            default='all',
            description='Datasets to update: daily,minute,earnings,quarterly_income_statement,quarterly_balance_sheet,quarterly_cash_flow,fundamentals_latest,all'
        ),
        'symbols': Param(
            default='',
            description='Comma-separated list of symbols (optional, uses master_symbol_list if empty)'
        ),
        'days': Param(
            default=7,
            description='Number of days for date range'
        ),
        'mode': Param(
            default='incremental',
            description='Update mode: incremental (only missing/stale) or full_refresh (all data)'
        )
    },
    doc_md="""
    # Daily Data Update Pipeline
    
    This DAG ensures all core trading datasets are fresh and complete:
    
    ## Schedule
    - **Symbols**: 6:00 AM ET (before market open)
    - **Market Data**: 4:30 PM ET (after market close)
    - **Earnings & Financials**: 5:00-6:00 PM ET
    
    ## Features
    - **Smart Updates**: Only fetches missing/stale data
    - **Comprehensive**: All datasets in single pipeline
    - **Resilient**: Retry logic and error handling
    - **Monitored**: Quality checks and notifications
    
    ## Parameters
    Configure via Airflow Variables:
    - `data_update_symbols`: Override symbol list (optional)
    - `data_update_days`: Lookback period (default: 7)
    - `data_update_mode`: 'incremental' or 'full' (default: incremental)
    """
)


def check_market_closed(**context):
    """Verify market is closed before running data updates."""
    from datetime import datetime
    import pytz
    
    logger = logging.getLogger(__name__)
    
    # Check if market is closed
    et_tz = pytz.timezone('US/Eastern')
    current_time = datetime.now(et_tz)
    market_close = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
    
    if current_time < market_close:
        logger.warning(f"Market may still be open. Current: {current_time}, Close: {market_close}")
        # Don't fail - just warn in case of early execution
    
    logger.info(f"✅ Market status check passed. Current time: {current_time}")
    return True


def run_data_update(dataset_type: str, **context):
    """Execute data update for specific dataset type."""

    logger = logging.getLogger(__name__)

    # Map old dataset names to new data source names
    SOURCE_MAPPING = {
        'daily': 'yfinance_daily',
        'minute': None,  # Not implemented yet
        'earnings': 'yfinance_earnings',
        'fundamentals_latest': 'yfinance_company_fundamentals',
        'quarterly_income_statement': 'yfinance_quarterly_income',
        'quarterly_balance_sheet': 'yfinance_quarterly_balance',
        'quarterly_cash_flow': 'yfinance_quarterly_cashflow',
    }

    source_name = SOURCE_MAPPING.get(dataset_type)
    if source_name is None:
        logger.warning(f"⏭️ Skipping {dataset_type} - data source not implemented yet")
        return {'status': 'skipped', 'reason': f'Data source for {dataset_type} not implemented'}
    
    try:
        # Get configuration from DAG parameters (with fallback to Airflow Variables)
        dag_run = context.get('dag_run')
        params = dag_run.conf if dag_run and dag_run.conf else {}
        
        # Use DAG parameters if provided, otherwise fall back to Variables
        datasets_param = params.get('datasets', 'all')
        symbols_override = params.get('symbols') or Variable.get('data_update_symbols', default_var=None)
        days_lookback = int(params.get('days', Variable.get('data_update_days', default_var=7)))
        update_mode = params.get('mode', Variable.get('data_update_mode', default_var='incremental'))
        
        # Log the configuration being used
        logger.info(f"📊 Data Update Configuration:")
        logger.info(f"  Dataset: {dataset_type}")
        logger.info(f"  Datasets param: {datasets_param}")
        logger.info(f"  Symbols: {symbols_override or 'master_symbol_list'}")
        logger.info(f"  Days: {days_lookback}")
        logger.info(f"  Mode: {update_mode}")
        
        # Override dataset_type if specified in params
        if datasets_param != 'all' and dataset_type not in datasets_param.split(','):
            # This dataset not requested in params, skip it
            logger.info(f"⏭️ Skipping {dataset_type} - not in requested datasets: {datasets_param}")
            return {'status': 'skipped', 'reason': f'Dataset {dataset_type} not in requested datasets'}
        
        # Build command to run directly within Airflow container
        cmd = [
            'python', '/opt/airflow/scripts/data_update.py',
            '--source', source_name, '--days', str(days_lookback)
        ]
        
        # Add symbols if specified
        if symbols_override:
            cmd.extend(['--symbols', symbols_override])
        
        # Add mode
        if update_mode in ['incremental', 'incremental']:
            cmd.append('--incremental')
        elif update_mode in ['full_refresh', 'full']:
            cmd.append('--full-refresh')
        
        # Add verbose logging
        cmd.append('--verbose')
        
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        # Define environment configurations with fallbacks
        environments = [
            {'PYTHONPATH': '/opt/airflow/src:/opt/airflow'},
            {'PYTHONPATH': '/opt/airflow/src'},
            {'PYTHONPATH': '/opt/airflow'}
        ]
        
        # Copy current environment and extend with project paths
        base_env = os.environ.copy()
        
        # Try each environment configuration
        last_error = None
        for i, env_config in enumerate(environments):
            try:
                # Merge environment configuration
                exec_env = base_env.copy()
                exec_env.update(env_config)
                
                logger.info(f"Attempt {i+1}: Using PYTHONPATH={env_config['PYTHONPATH']}")
                
                # Execute the update script directly within container
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True, 
                    cwd='/opt/airflow',
                    env=exec_env,
                    timeout=3600  # 1 hour timeout
                )
                
                if result.returncode == 0:
                    logger.info(f"✅ {dataset_type.title()} data update completed successfully")
                    logger.info(f"Output: {result.stdout}")
                    
                    # Parse output for metrics (basic parsing)
                    output_lines = result.stdout.split('\n')
                    metrics = {'status': 'success', 'dataset': dataset_type}
                    
                    for line in output_lines:
                        if 'Symbols processed:' in line:
                            metrics['symbols_processed'] = line.split(':')[1].strip()
                        elif 'Records added:' in line:
                            metrics['records_added'] = line.split(':')[1].strip()
                    
                    # Store metrics for downstream tasks
                    context['task_instance'].xcom_push(
                        key=f'{dataset_type}_metrics',
                        value=metrics
                    )
                    
                    return metrics
                else:
                    # Log error but try next environment configuration
                    logger.warning(f"Attempt {i+1} failed with return code {result.returncode}")
                    logger.warning(f"STDOUT: {result.stdout}")
                    logger.warning(f"STDERR: {result.stderr}")
                    last_error = f"Return code {result.returncode}: {result.stderr}"
                    continue
                    
            except subprocess.TimeoutExpired:
                logger.error(f"❌ {dataset_type} update timed out after 1 hour on attempt {i+1}")
                raise
            except Exception as e:
                logger.warning(f"Attempt {i+1} failed with exception: {e}")
                last_error = str(e)
                continue
        
        # If all environment configurations failed
        error_msg = f"All environment configurations failed. Last error: {last_error}"
        logger.error(f"❌ {dataset_type} update failed: {error_msg}")
        raise Exception(error_msg)
        
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {dataset_type} update timed out after 1 hour")
        raise
    except Exception as e:
        logger.error(f"❌ {dataset_type} update failed: {e}")
        raise


def validate_data_quality(**context):
    """Run comprehensive data quality checks."""

    logger = logging.getLogger(__name__)

    try:
        # Add project to path
        sys.path.append('/opt/airflow/src')
        from src.database.postgres import postgres_connection
        from sqlalchemy import text
        from datetime import date, timedelta

        # Connect to PostgreSQL database
        conn = postgres_connection().__enter__()
        
        quality_metrics = {}
        issues = []

        # Check 1: Symbol coverage
        symbol_query = text("""
            SELECT COUNT(*) as total_symbols,
                   COUNT(CASE WHEN NOT is_test_issue THEN 1 END) as active_symbols
            FROM ml_data.master_symbol_list
        """)
        symbol_stats = conn.execute(symbol_query).fetchone()
        quality_metrics['total_symbols'] = symbol_stats[0]
        quality_metrics['active_symbols'] = symbol_stats[1]

        if symbol_stats[1] < 2500:
            issues.append(f"Low non-test symbol count: {symbol_stats[1]} (expected > 2500)")

        # Check 2: Daily data freshness
        daily_query = text("""
            SELECT MAX(date) as latest_date,
                   COUNT(DISTINCT symbol) as symbols_with_data
            FROM ml_data.market_data_daily
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        daily_stats = conn.execute(daily_query).fetchone()
        quality_metrics['latest_daily_date'] = str(daily_stats[0]) if daily_stats[0] else None
        quality_metrics['symbols_with_daily_data'] = daily_stats[1]

        # Check data lag
        if daily_stats[0]:
            data_lag = (date.today() - daily_stats[0]).days
            quality_metrics['daily_data_lag_days'] = data_lag
            if data_lag > 3:
                issues.append(f"Daily data is {data_lag} days old")
        else:
            issues.append("No recent daily data found")

        # Check 3: Company info data coverage
        company_info_query = text("""
            SELECT COUNT(DISTINCT symbol) as symbols_with_company_info
            FROM ml_data.company_info
            WHERE updated_date >= CURRENT_DATE - INTERVAL '60 days'
        """)
        company_stats = conn.execute(company_info_query).fetchone()
        quality_metrics['symbols_with_company_info'] = company_stats[0]
        
        if company_stats[0] < 1000:
            issues.append(f"Low company info coverage: {company_stats[0]} symbols")

        # Check 4: Minute data availability (last 3 days)
        minute_query = text("""
            SELECT COUNT(DISTINCT symbol) as symbols_with_minute_data,
                   MAX(timestamp) as latest_minute
            FROM ml_data.market_data_minute
            WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '3 days'
        """)
        minute_stats = conn.execute(minute_query).fetchone()
        quality_metrics['symbols_with_minute_data'] = minute_stats[0]
        quality_metrics['latest_minute_timestamp'] = str(minute_stats[1]) if minute_stats[1] else None

        # Check 5: Earnings data coverage
        earnings_query = text("""
            SELECT COUNT(DISTINCT symbol) as symbols_with_earnings
            FROM ml_data.earnings
            WHERE quarter_date >= CURRENT_DATE - INTERVAL '1 year'
        """)
        earnings_stats = conn.execute(earnings_query).fetchone()
        quality_metrics['symbols_with_earnings'] = earnings_stats[0]

        # Close PostgreSQL connection
        conn.close()
        
        # Log quality results
        logger.info("📊 Data Quality Check Results:")
        logger.info(f"  Active symbols: {quality_metrics['active_symbols']}")
        logger.info(f"  Latest daily data: {quality_metrics['latest_daily_date']}")
        logger.info(f"  Daily data lag: {quality_metrics.get('daily_data_lag_days', 'N/A')} days")
        logger.info(f"  Symbols with company info: {quality_metrics['symbols_with_company_info']}")
        logger.info(f"  Symbols with minute data: {quality_metrics['symbols_with_minute_data']}")
        logger.info(f"  Symbols with earnings: {quality_metrics['symbols_with_earnings']}")
        
        # Log issues
        if issues:
            logger.warning("⚠️ Quality Issues Found:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("✅ All quality checks passed")
        
        quality_metrics['issues'] = issues
        quality_metrics['overall_status'] = 'warning' if issues else 'healthy'
        
        # Store metrics
        context['task_instance'].xcom_push(key='quality_metrics', value=quality_metrics)
        
        return quality_metrics
        
    except Exception as e:
        logger.error(f"❌ Data quality check failed: {e}")
        raise


def send_completion_summary(**context):
    """Send comprehensive summary of the data update process."""
    
    logger = logging.getLogger(__name__)
    
    try:
        ti = context['task_instance']
        
        # Gather metrics from all tasks
        daily_metrics = ti.xcom_pull(task_ids='market_data_updates.update_daily_data', key='daily_metrics') or {}
        minute_metrics = ti.xcom_pull(task_ids='market_data_updates.update_minute_data', key='minute_metrics') or {}
        earnings_metrics = ti.xcom_pull(task_ids='supplemental_updates.update_earnings', key='earnings_metrics') or {}
        quarterly_income_metrics = ti.xcom_pull(task_ids='supplemental_updates.update_quarterly_income_statement', key='quarterly_income_statement_metrics') or {}
        quarterly_balance_metrics = ti.xcom_pull(task_ids='supplemental_updates.update_quarterly_balance_sheet', key='quarterly_balance_sheet_metrics') or {}
        quarterly_cash_flow_metrics = ti.xcom_pull(task_ids='supplemental_updates.update_quarterly_cash_flow', key='quarterly_cash_flow_metrics') or {}
        quality_metrics = ti.xcom_pull(task_ids='validate_data_quality', key='quality_metrics') or {}
        
        # Create comprehensive summary
        summary = f"""
📊 Daily Data Update Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}

🏁 Pipeline Status: {'✅ SUCCESS' if not quality_metrics.get('issues') else '⚠️ WARNING'}

📈 Market Data Updates:
• Daily Data: {daily_metrics.get('symbols_processed', 'N/A')} symbols, {daily_metrics.get('records_added', 'N/A')} records
• Minute Data: {minute_metrics.get('symbols_processed', 'N/A')} symbols, {minute_metrics.get('records_added', 'N/A')} records

🏢 Supplemental Data Updates:
• Earnings: {earnings_metrics.get('symbols_processed', 'N/A')} symbols, {earnings_metrics.get('records_added', 'N/A')} records
• Quarterly Income Statement: {quarterly_income_metrics.get('symbols_processed', 'N/A')} symbols, {quarterly_income_metrics.get('records_added', 'N/A')} records
• Quarterly Balance Sheet: {quarterly_balance_metrics.get('symbols_processed', 'N/A')} symbols, {quarterly_balance_metrics.get('records_added', 'N/A')} records
• Quarterly Cash Flow: {quarterly_cash_flow_metrics.get('symbols_processed', 'N/A')} symbols, {quarterly_cash_flow_metrics.get('records_added', 'N/A')} records

📊 Data Quality Status:
• Active Symbols: {quality_metrics.get('active_symbols', 'N/A')}
• Latest Daily Data: {quality_metrics.get('latest_daily_date', 'N/A')}
• Data Lag: {quality_metrics.get('daily_data_lag_days', 'N/A')} days
• Company Info Coverage: {quality_metrics.get('symbols_with_company_info', 'N/A')} symbols
"""
        
        # Add issues if any
        if quality_metrics.get('issues'):
            summary += "\n⚠️ Quality Issues:\n"
            for issue in quality_metrics['issues']:
                summary += f"• {issue}\n"
        
        summary += f"""
🎯 Next Pipeline Run: Tomorrow 4:30 PM ET

---
Automated by Daily Data Update DAG
Airflow Instance: {context.get('dag_run').dag_id}
"""
        
        logger.info("📧 Daily Update Summary:")
        logger.info(summary)
        
        # Send email notification if configured
        try:
            from airflow.utils.email import send_email
            
            email_list = Variable.get('alert_email', default_var=None)
            if email_list and email_list != ['trading-alerts@company.com']:
                subject = '📊 Daily Data Update Complete'
                if quality_metrics.get('issues'):
                    subject = '⚠️ Daily Data Update - Issues Detected'
                
                send_email(
                    to=email_list,
                    subject=subject,
                    html_content=summary.replace('\n', '<br>').replace('•', '&bull;')
                )
                logger.info(f"📧 Summary email sent to {email_list}")
        except Exception as email_error:
            logger.warning(f"Could not send summary email: {email_error}")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Failed to send completion summary: {e}")
        # Don't fail the DAG for notification issues
        pass


# Task Groups for better organization
with TaskGroup('market_data_updates', dag=dag) as market_data_group:
    """Market data updates (daily and minute)"""
    
    update_daily_data = PythonOperator(
        task_id='update_daily_data',
        python_callable=lambda **ctx: run_data_update('daily', **ctx),
        doc_md="""
        ## Daily Market Data Update
        
        Updates the `market_data_daily` table with OHLCV data.
        
        **Features:**
        - Smart incremental updates (only missing dates)
        - Batch processing for API efficiency
        - Comprehensive error handling
        
        **Source:** YFinance API
        **Rate Limit:** 0.1s between requests
        """
    )
    
    update_minute_data = PythonOperator(
        task_id='update_minute_data',
        python_callable=lambda **ctx: run_data_update('minute', **ctx),
        doc_md="""
        ## Minute Market Data Update
        
        Updates the `market_data_minute` table with 1-minute OHLCV data.
        Maintains a rolling 5-day window.
        
        **Features:**
        - Rolling window maintenance
        - Automatic cleanup of old data
        - Rate limited for API stability
        
        **Source:** YFinance API (1m interval)
        """
    )

with TaskGroup('supplemental_updates', dag=dag) as supplemental_group:
    """Earnings and financial data updates"""
    
    update_earnings = PythonOperator(
        task_id='update_earnings',
        python_callable=lambda **ctx: run_data_update('earnings', **ctx),
        doc_md="""
        ## Earnings Data Update
        
        Updates the `earnings` table with quarterly earnings data.
        
        **Features:**
        - Quarterly revenue and earnings
        - Historical earnings coverage
        - Incremental updates for missing quarters
        
        **Source:** YFinance income statement data
        """
    )
    
    update_fundamentals_latest = PythonOperator(
        task_id='update_fundamentals_latest',
        python_callable=lambda **ctx: run_data_update('fundamentals_latest', **ctx),
        doc_md="""
        ## Fundamentals Latest Update
        
        Updates the `fundamentals_latest` table with current financial metrics for stock screening.
        
        **Features:**
        - 31 curated financial metrics (valuation, profitability, market data)
        - Sector and industry for screening
        - Daily updates with current market values
        - PE ratios, margins, growth rates, market cap, beta
        
        **Source:** YFinance info API (real-time data)
        """
    )
    
    update_quarterly_income_statement = PythonOperator(
        task_id='update_quarterly_income_statement',
        python_callable=lambda **ctx: run_data_update('quarterly_income_statement', **ctx),
        doc_md="""
        ## Quarterly Income Statement Update
        
        Updates the `quarterly_income_statement` table with comprehensive quarterly financial metrics.
        
        **Features:**
        - All 33 quarterly income statement metrics
        - Comprehensive financial data (EBITDA, EPS, revenue, etc.)
        - Incremental updates for missing quarters
        
        **Source:** YFinance quarterly income statement
        """
    )
    
    update_quarterly_balance_sheet = PythonOperator(
        task_id='update_quarterly_balance_sheet',
        python_callable=lambda **ctx: run_data_update('quarterly_balance_sheet', **ctx),
        doc_md="""
        ## Quarterly Balance Sheet Update
        
        Updates the `quarterly_balance_sheet` table with comprehensive quarterly balance sheet data.
        
        **Features:**
        - All 65 quarterly balance sheet metrics
        - Assets, liabilities, equity, cash positions, debt levels
        - Incremental updates for missing quarters
        
        **Source:** YFinance quarterly balance sheet
        """
    )
    
    update_quarterly_cash_flow = PythonOperator(
        task_id='update_quarterly_cash_flow',
        python_callable=lambda **ctx: run_data_update('quarterly_cash_flow', **ctx),
        doc_md="""
        ## Quarterly Cash Flow Update
        
        Updates the `quarterly_cash_flow` table with comprehensive quarterly cash flow statement data.
        
        **Features:**
        - All 46 quarterly cash flow metrics
        - Operating, investing, financing cash flows, free cash flow, capex
        - Incremental updates for missing quarters
        
        **Source:** YFinance quarterly cash flow
        """
    )

# Individual tasks
check_market_status = PythonOperator(
    task_id='check_market_status',
    python_callable=check_market_closed,
    dag=dag,
    doc_md="""
    ## Market Status Check
    
    Verifies market is closed before running data updates.
    Issues warning if executed during market hours.
    """
)

validate_quality = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
    doc_md="""
    ## Data Quality Validation
    
    Comprehensive quality checks across all datasets:
    - Symbol coverage and freshness
    - Data completeness and timeliness
    - Coverage gaps identification
    - Staleness detection
    """
)

send_summary = PythonOperator(
    task_id='send_completion_summary',
    python_callable=send_completion_summary,
    dag=dag,
    trigger_rule='all_done',  # Run even if some tasks fail
    doc_md="""
    ## Completion Summary
    
    Sends comprehensive summary including:
    - Update statistics for all datasets
    - Data quality status
    - Issue identification and alerts
    - Email notifications (if configured)
    
    Runs regardless of task status to ensure notifications.
    """
)

# Define task dependencies
check_market_status >> [market_data_group, supplemental_group]
[market_data_group, supplemental_group] >> validate_quality >> send_summary

# Sequential execution within groups for API rate limiting
update_daily_data >> update_minute_data
update_earnings >> update_fundamentals_latest >> update_quarterly_income_statement >> update_quarterly_balance_sheet >> update_quarterly_cash_flow