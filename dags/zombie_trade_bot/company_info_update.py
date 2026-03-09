"""
Company Info Update DAG

Dedicated Airflow DAG for updating static company information in PostgreSQL.
Runs monthly to update business data that changes infrequently.

Updated Table:
- company_info (6.1) - Static business information from YFinance info endpoint

Features:
- Monthly scheduling (1st of month - business data changes infrequently)
- Focus on static business data only (no market data)
- Comprehensive error handling and retry logic
- Data quality validation
- Summary notifications
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Param
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
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=4),  # Company info takes longer to collect
}

# DAG definition
dag = DAG(
    'company_info_update',
    default_args=default_args,
    description='Monthly static company information update pipeline',
    schedule_interval='0 20 1 * *',  # 8:00 PM ET on 1st of each month
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=['data', 'company-info', 'static', 'monthly'],
    params={
        'symbols': Param(
            default='',
            description='Comma-separated list of symbols (optional, uses master_symbol_list if empty)'
        ),
        'mode': Param(
            default='incremental',
            description='Update mode: incremental (only stale/missing) or full_refresh (all data)'
        ),
        'max_daily_calls': Param(
            default=100,
            description='Maximum API calls per day (default: 100, set higher for full universe updates)'
        )
    },
    doc_md="""
    # Company Info Update Pipeline
    
    This DAG ensures static company information is fresh and complete:
    
    ## Schedule
    - **Monthly**: 1st of month at 8:00 PM ET (business data changes infrequently)
    
    ## Updated Table
    - **company_info**: Static business information (13 columns)
      - Business: sector, industry, business_summary, website, employees
      - Location: country, city, state, currency
      - Identifiers: short_name, long_name
    
    ## Features
    - **Static Data Focus**: No market data (market cap, P/E ratios, etc.)
    - **Monthly Updates**: Business information changes infrequently
    - **Comprehensive**: All static fields from YFinance info endpoint
    - **Resilient**: Retry logic and error handling
    - **Monitored**: Quality checks and notifications
    
    ## Parameters
    Configure via Airflow Variables:
    - `company_info_symbols`: Override symbol list (optional)
    - `company_info_mode`: 'incremental' or 'full' (default: incremental)
    """
)


def run_company_info_update(**context):
    """Execute company info data update."""
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get configuration from DAG parameters (with fallback to Airflow Variables)
        dag_run = context.get('dag_run')
        params = dag_run.conf if dag_run and dag_run.conf else {}
        
        # Use DAG parameters if provided, otherwise fall back to Variables
        symbols_override = params.get('symbols') or Variable.get('company_info_symbols', default_var=None)
        update_mode = params.get('mode', Variable.get('company_info_mode', default_var='incremental'))
        max_daily_calls = params.get('max_daily_calls', Variable.get('company_info_max_daily_calls', default_var=100))
        
        # Log the configuration being used
        logger.info(f"📊 Company Info Update Configuration:")
        logger.info(f"  Symbols: {symbols_override or 'master_symbol_list'}")
        logger.info(f"  Mode: {update_mode}")
        logger.info(f"  Daily API limit: {max_daily_calls}")
        
        # Build command to run directly within Airflow container
        cmd = [
            'python', '/opt/airflow/scripts/data_update.py',
            '--source', 'yfinance_company_fundamentals'
        ]

        # Add symbols if specified
        if symbols_override:
            cmd.extend(['--symbols', symbols_override])

        # Add mode
        if update_mode in ['full_refresh', 'full']:
            cmd.append('--full-refresh')
        # Default is incremental, no flag needed

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
                    timeout=7200  # 2 hour timeout (company info takes longer)
                )
                
                if result.returncode == 0:
                    logger.info(f"✅ Company info update completed successfully")
                    logger.info(f"Output: {result.stdout}")
                    
                    # Parse output for metrics (basic parsing)
                    output_lines = result.stdout.split('\n')
                    metrics = {'status': 'success', 'dataset': 'company_info'}
                    
                    for line in output_lines:
                        if 'Symbols processed:' in line:
                            metrics['symbols_processed'] = line.split(':')[1].strip()
                        elif 'Records added:' in line:
                            metrics['records_added'] = line.split(':')[1].strip()
                    
                    # Store metrics for downstream tasks
                    context['task_instance'].xcom_push(
                        key='company_info_metrics',
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
                logger.error(f"❌ Company info update timed out after 2 hours on attempt {i+1}")
                raise
            except Exception as e:
                logger.warning(f"Attempt {i+1} failed with exception: {e}")
                last_error = str(e)
                continue
        
        # If all environment configurations failed
        error_msg = f"All environment configurations failed. Last error: {last_error}"
        logger.error(f"❌ Company info update failed: {error_msg}")
        raise Exception(error_msg)
        
    except subprocess.TimeoutExpired:
        logger.error(f"❌ Company info update timed out after 2 hours")
        raise
    except Exception as e:
        logger.error(f"❌ Company info update failed: {e}")
        raise


def validate_company_info_quality(**context):
    """Run data quality checks on company info table."""
    
    logger = logging.getLogger(__name__)
    
    try:
        # Add project to path
        sys.path.append('/opt/airflow/src')
        from src.database.postgres import postgres_connection
        from sqlalchemy import text
        from datetime import date, timedelta
        
        # Connect to database
        conn = postgres_connection().__enter__()
        quality_metrics = {}
        issues = []
        
        # Check 1: Company info coverage
        coverage_query = """
            SELECT COUNT(*) as symbols_with_company_info,
                   COUNT(CASE WHEN sector IS NOT NULL THEN 1 END) as symbols_with_sector,
                   COUNT(CASE WHEN business_summary IS NOT NULL THEN 1 END) as symbols_with_summary,
                   COUNT(CASE WHEN employees IS NOT NULL THEN 1 END) as symbols_with_employees,
                   MAX(updated_date) as latest_update
            FROM ml_data.company_info
            WHERE updated_date >= CURRENT_DATE - INTERVAL '60 days'
        """
        coverage_stats = conn.execute(text(coverage_query)).fetchone()
        quality_metrics['symbols_with_company_info'] = coverage_stats[0]
        quality_metrics['symbols_with_sector'] = coverage_stats[1]
        quality_metrics['symbols_with_summary'] = coverage_stats[2]
        quality_metrics['symbols_with_employees'] = coverage_stats[3]
        quality_metrics['latest_company_info_update'] = str(coverage_stats[4]) if coverage_stats[4] else None
        
        if coverage_stats[0] < 500:
            issues.append(f"Low company info coverage: {coverage_stats[0]} symbols")
        
        # Check 2: Sector distribution
        sector_query = """
            SELECT sector, COUNT(*) as count
            FROM ml_data.company_info
            WHERE sector IS NOT NULL
            GROUP BY sector
            ORDER BY count DESC
            LIMIT 5
        """
        sector_stats = conn.execute(text(sector_query)).fetchall()
        quality_metrics['top_sectors'] = {row[0]: row[1] for row in sector_stats}
        
        # Check 3: Data freshness (company info should be updated monthly)
        freshness_threshold = date.today() - timedelta(days=45)  # 45 days threshold
        
        if quality_metrics['latest_company_info_update']:
            latest_date = datetime.strptime(quality_metrics['latest_company_info_update'].split(' ')[0], '%Y-%m-%d').date()
            if latest_date < freshness_threshold:
                days_old = (date.today() - latest_date).days
                issues.append(f"Company info data is {days_old} days old")
        
        # Check 4: Required field completeness
        required_fields = ['sector', 'industry']
        for field in required_fields:
            field_query = f"""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN {field} IS NOT NULL AND {field} != '' THEN 1 END) as with_data
                FROM ml_data.company_info
            """
            field_stats = conn.execute(text(field_query)).fetchone()
            completeness = (field_stats[1] / field_stats[0] * 100) if field_stats[0] > 0 else 0
            quality_metrics[f'{field}_completeness'] = round(completeness, 1)
            
            if completeness < 80:
                issues.append(f"Low {field} completeness: {completeness}%")
        
        conn.close()
        
        # Log quality results
        logger.info("📊 Company Info Quality Check Results:")
        logger.info(f"  Symbols with company info: {quality_metrics['symbols_with_company_info']}")
        logger.info(f"  Symbols with sector: {quality_metrics['symbols_with_sector']}")
        logger.info(f"  Symbols with business summary: {quality_metrics['symbols_with_summary']}")
        logger.info(f"  Symbols with employee count: {quality_metrics['symbols_with_employees']}")
        logger.info(f"  Latest update: {quality_metrics['latest_company_info_update']}")
        logger.info(f"  Sector completeness: {quality_metrics.get('sector_completeness', 'N/A')}%")
        logger.info(f"  Industry completeness: {quality_metrics.get('industry_completeness', 'N/A')}%")
        
        # Log issues
        if issues:
            logger.warning("⚠️ Quality Issues Found:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("✅ All company info quality checks passed")
        
        quality_metrics['issues'] = issues
        quality_metrics['overall_status'] = 'warning' if issues else 'healthy'
        
        # Store metrics
        context['task_instance'].xcom_push(key='company_info_quality_metrics', value=quality_metrics)
        
        return quality_metrics
        
    except Exception as e:
        logger.error(f"❌ Company info quality check failed: {e}")
        raise


def send_company_info_summary(**context):
    """Send comprehensive summary of the company info update process."""
    
    logger = logging.getLogger(__name__)
    
    try:
        ti = context['task_instance']
        
        # Gather metrics from all tasks
        company_info_metrics = ti.xcom_pull(task_ids='update_company_info', key='company_info_metrics') or {}
        quality_metrics = ti.xcom_pull(task_ids='validate_company_info_quality', key='company_info_quality_metrics') or {}
        
        # Create comprehensive summary
        summary = f"""
🏢 Company Info Update Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}

🏁 Pipeline Status: {'✅ SUCCESS' if not quality_metrics.get('issues') else '⚠️ WARNING'}

📊 Company Info Updates:
• Symbols Processed: {company_info_metrics.get('symbols_processed', 'N/A')}
• Records Updated: {company_info_metrics.get('records_added', 'N/A')}

📊 Data Quality Status:
• Total Coverage: {quality_metrics.get('symbols_with_company_info', 'N/A')} symbols
• Sector Coverage: {quality_metrics.get('symbols_with_sector', 'N/A')} symbols
• Business Summary: {quality_metrics.get('symbols_with_summary', 'N/A')} symbols
• Employee Data: {quality_metrics.get('symbols_with_employees', 'N/A')} symbols
• Sector Completeness: {quality_metrics.get('sector_completeness', 'N/A')}%
• Industry Completeness: {quality_metrics.get('industry_completeness', 'N/A')}%

📈 Top Sectors:
"""
        
        # Add top sectors
        if quality_metrics.get('top_sectors'):
            for sector, count in quality_metrics['top_sectors'].items():
                summary += f"• {sector}: {count} companies\n"
        
        # Add issues if any
        if quality_metrics.get('issues'):
            summary += "\n⚠️ Quality Issues:\n"
            for issue in quality_metrics['issues']:
                summary += f"• {issue}\n"
        
        summary += f"""
🎯 Next Pipeline Run: Next month (1st at 8:00 PM ET)

---
Automated by Company Info Update DAG
Airflow Instance: {context.get('dag_run').dag_id}
"""
        
        logger.info("📧 Company Info Update Summary:")
        logger.info(summary)
        
        # Send email notification if configured
        try:
            from airflow.utils.email import send_email
            
            email_list = Variable.get('alert_email', default_var=None)
            if email_list and email_list != ['trading-alerts@company.com']:
                subject = '🏢 Company Info Update Complete'
                if quality_metrics.get('issues'):
                    subject = '⚠️ Company Info Update - Issues Detected'
                
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
        logger.error(f"❌ Failed to send company info completion summary: {e}")
        # Don't fail the DAG for notification issues
        pass


# Individual tasks
update_company_info = PythonOperator(
    task_id='update_company_info',
    python_callable=run_company_info_update,
    dag=dag,
    doc_md="""
    ## Company Info Update
    
    Updates the `company_info` table with static business information.
    
    **Features:**
    - Static business data only (no market metrics)
    - Monthly update frequency (business data changes infrequently)
    - Comprehensive business fields: sector, industry, employees, website
    - Location data: country, city, state, currency
    - Company identifiers: short_name, long_name
    
    **Source:** YFinance info endpoint (static fields only)
    **Rate Limit:** 2x slower than normal (monthly updates don't need speed)
    """
)

validate_quality = PythonOperator(
    task_id='validate_company_info_quality',
    python_callable=validate_company_info_quality,
    dag=dag,
    doc_md="""
    ## Company Info Quality Validation
    
    Comprehensive quality checks for company info data:
    - Coverage analysis (symbols with complete data)
    - Sector distribution validation
    - Required field completeness
    - Data freshness monitoring
    """
)

send_summary = PythonOperator(
    task_id='send_company_info_summary',
    python_callable=send_company_info_summary,
    dag=dag,
    trigger_rule='all_done',  # Run even if some tasks fail
    doc_md="""
    ## Company Info Completion Summary
    
    Sends comprehensive summary including:
    - Update statistics for company info
    - Data quality status and coverage
    - Sector distribution analysis
    - Issue identification and alerts
    - Email notifications (if configured)
    
    Runs regardless of task status to ensure notifications.
    """
)

# Define task dependencies
update_company_info >> validate_quality >> send_summary