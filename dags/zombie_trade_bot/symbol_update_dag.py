"""
Symbol Update DAG

Daily automated symbol list update from NASDAQ FTP server.
Downloads fresh symbol lists and updates the master_symbol_list table.

This DAG:
- Downloads nasdaqlisted.txt and otherlisted.txt from NASDAQ FTP
- Updates master_symbol_list table with new/changed symbols
- Marks delisted symbols as inactive
- Sends alerts on significant changes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.email import send_email
import logging
import sys
import os

# Add project to path
sys.path.append('/opt/airflow/src')

# Default arguments
default_args = {
    'owner': 'trading-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': Variable.get('alert_email', default_var=['trading-alerts@company.com']),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'symbol_update',
    default_args=default_args,
    description='Daily NASDAQ symbol list update from FTP server',
    schedule_interval='0 6 * * 1-5',  # 6:00 AM ET on weekdays (before market open)
    catchup=False,
    max_active_runs=1,
    tags=['symbols', 'nasdaq', 'daily', 'universe']
)


def check_ftp_availability(**context):
    """Check if NASDAQ FTP server is accessible before running updates."""
    
    import ftplib
    
    logger = logging.getLogger(__name__)
    
    try:
        # Test FTP connection
        ftp = ftplib.FTP('ftp.nasdaqtrader.com')
        ftp.login('anonymous', '')
        ftp.cwd('SymbolDirectory')
        
        # Check if files exist
        files = ftp.nlst()
        required_files = ['nasdaqlisted.txt', 'otherlisted.txt']
        
        missing_files = [f for f in required_files if f not in files]
        if missing_files:
            raise Exception(f"Required files missing from FTP: {missing_files}")
        
        ftp.quit()
        logger.info("✅ NASDAQ FTP server is accessible and files are available")
        return True
        
    except Exception as e:
        logger.error(f"❌ NASDAQ FTP check failed: {e}")
        raise



def validate_symbol_update(**context):
    """Validate that the symbol update was successful."""
    
    import sys
    sys.path.append('/opt/airflow/src')
    
    from src.database.postgres import postgres_connection
        from sqlalchemy import text
    
    logger = logging.getLogger(__name__)
    
    try:
        # Connect to database
        conn = postgres_connection().__enter__()
                # Get symbol statistics
        stats_query = """
        SELECT 
            COUNT(*) as total_symbols,
            COUNT(CASE WHEN NOT is_test_issue THEN 1 END) as active_symbols,
            COUNT(CASE WHEN is_test_issue THEN 1 END) as test_issues,
            COUNT(CASE WHEN is_etf THEN 1 END) as etf_count,
            MAX(file_creation_time) as last_update_time
        FROM ml_data.master_symbol_list
        """
        
        stats = conn.execute(text(stats_query)).fetchone()
        
        if not stats or stats[0] == 0:
            raise Exception("No symbols found in master_symbol_list table")
        
        # Validate reasonable numbers
        total_symbols = stats[0]
        active_symbols = stats[1]  # Now non-test symbols
        
        if total_symbols < 3000:  # Expect at least 3000 US symbols
            raise Exception(f"Symbol count too low: {total_symbols} (expected > 3000)")
        
        if active_symbols < 2500:  # Most should be non-test symbols
            raise Exception(f"Non-test symbol count too low: {active_symbols} (expected > 2500)")
        
        # Check update time is recent (within last 2 hours)
        last_update = stats[4]  # Adjusted index after removing columns
        if last_update:
            from datetime import datetime, timedelta
            try:
                # Handle different timestamp formats from database
                if isinstance(last_update, str):
                    update_time = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                else:
                    # Assume it's already a datetime object or timestamp
                    update_time = last_update if hasattr(last_update, 'year') else datetime.fromtimestamp(last_update)
                
                if datetime.now() - update_time > timedelta(hours=2):
                    logger.warning(f"Last update was {update_time}, which is more than 2 hours ago")
            except Exception as e:
                logger.warning(f"Could not parse last_update timestamp: {last_update} ({e})")
        
        logger.info(f"✅ Symbol validation passed:")
        logger.info(f"  Total symbols: {stats[0]}")
        logger.info(f"  Non-test symbols: {stats[1]}")
        logger.info(f"  Test issues: {stats[2]}")
        logger.info(f"  ETFs: {stats[3]}")
        logger.info(f"  Last file creation time: {stats[4]}")
        
        # Get exchange distribution
        exchange_query = """
        SELECT exchange, COUNT(*) as count 
        FROM ml_data.master_symbol_list 
        WHERE NOT is_test_issue
        GROUP BY exchange 
        ORDER BY count DESC
        """
        
        exchanges = conn.execute(text(exchange_query)).fetchall()
        logger.info("Exchange distribution:")
        for exchange, count in exchanges:
            logger.info(f"  {exchange}: {count}")
        
        conn.close()
        
        return {
            'validation_status': 'passed',
            'total_symbols': stats[0],
            'active_symbols': stats[1],
            'last_updated': stats[4]
        }
        
    except Exception as e:
        logger.error(f"❌ Symbol validation failed: {e}")
        raise


def send_update_summary(**context):
    """Send summary email of the symbol update process."""
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get results from previous tasks
        ti = context['ti']
        validation_result = ti.xcom_pull(task_ids='validate_update')
        
        if not validation_result:
            logger.warning("No validation results available for summary")
            return
        
        # Create summary
        summary = f"""
        📊 Daily Symbol Update Summary - {datetime.now().strftime('%Y-%m-%d %H:%M')}
        
        ✅ Symbol update completed successfully
        
        📈 Statistics:
        • Total symbols: {validation_result.get('total_symbols', 'N/A')}
        • Active symbols: {validation_result.get('active_symbols', 'N/A')}
        • Last updated: {validation_result.get('last_updated', 'N/A')}
        
        🎯 This ensures our trading universe has the latest symbol data from NASDAQ.
        
        ---
        Automated by Airflow Symbol Update DAG
        """
        
        logger.info("📧 Symbol update summary:")
        logger.info(summary)
        
        # Only send email if configured
        try:
            email_list = Variable.get('alert_email', default_var=None)
            if email_list and email_list != ['trading-alerts@company.com']:
                send_email(
                    to=email_list,
                    subject='✅ Daily Symbol Update Complete',
                    html_content=summary.replace('\n', '<br>')
                )
                logger.info(f"📧 Summary email sent to {email_list}")
        except Exception as email_error:
            logger.warning(f"Could not send summary email: {email_error}")
        
    except Exception as e:
        logger.error(f"❌ Failed to send update summary: {e}")
        # Don't fail the DAG for email issues
        pass


# Define tasks
check_ftp_task = PythonOperator(
    task_id='check_ftp_availability',
    python_callable=check_ftp_availability,
    dag=dag,
    doc_md="""
    ## Check FTP Availability
    
    Verifies that the NASDAQ FTP server is accessible and required files are available
    before attempting to download symbol data.
    
    **Checks:**
    - FTP server connectivity
    - Presence of nasdaqlisted.txt and otherlisted.txt files
    """
)

fetch_symbols_task = BashOperator(
    task_id='fetch_and_update_symbols',
    bash_command='cd /opt/airflow && python scripts/ml_fetch_symbols.py',
    dag=dag,
    doc_md="""
    ## Fetch and Update Symbols

    Executes the main symbol fetch script directly in the Airflow container that:
    - Downloads fresh symbol data from NASDAQ FTP
    - Parses nasdaqlisted.txt and otherlisted.txt
    - Updates the master_symbol_list table in PostgreSQL
    - Marks inactive symbols appropriately
    """
)

validate_update_task = PythonOperator(
    task_id='validate_update',
    python_callable=validate_symbol_update,
    dag=dag,
    doc_md="""
    ## Validate Symbol Update
    
    Performs quality checks on the updated symbol data:
    - Verifies reasonable symbol counts
    - Checks update timestamps
    - Validates exchange distribution
    - Ensures data integrity
    """
)

send_summary_task = PythonOperator(
    task_id='send_update_summary',
    python_callable=send_update_summary,
    dag=dag,
    trigger_rule='all_done',  # Run even if previous tasks fail
    doc_md="""
    ## Send Update Summary
    
    Sends a summary of the symbol update process including:
    - Update status
    - Symbol statistics
    - Any issues encountered
    
    Runs regardless of previous task status to ensure notifications are sent.
    """
)

# Define task dependencies
check_ftp_task >> fetch_symbols_task >> validate_update_task >> send_summary_task