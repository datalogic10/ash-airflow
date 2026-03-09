"""
ML Feature Pre-computation DAG

Pre-computes static and semi-static features to improve performance.
Runs daily after market close to prepare features for the next trading day.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from features.ml_dataset_service import MLDatasetService
from features.feature_registry import feature_registry, FeatureType

logger = logging.getLogger(__name__)

# DAG Configuration
DAG_ID = 'ml_feature_precompute'
SCHEDULE_INTERVAL = '0 18 * * 1-5'  # 6 PM ET, Monday-Friday (before sync)
START_DATE = days_ago(1)

default_args = {
    'owner': 'zombie-trade-bot',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


def get_active_symbols_task(**context):
    """
    Get list of active trading symbols from the universe.

    Args:
        context: Airflow context

    Returns:
        List of active symbols
    """
    logger.info("Getting active symbols for pre-computation")

    try:
        # Import PostgreSQL connection
        from src.database.postgres import postgres_connection
        from sqlalchemy import text

        # Connect to PostgreSQL database
        with postgres_connection() as conn:
            # Get active symbols from master symbol list
            query = text("""
                SELECT symbol
                FROM ml_data.master_symbol_list
                WHERE is_test_issue = FALSE
                AND exchange IN ('NASDAQ', 'NYSE', 'AMEX')
                ORDER BY symbol
                LIMIT 500
            """)

            result = conn.execute(query).fetchall()
            symbols = [row[0] for row in result]

        logger.info(f"Found {len(symbols)} active symbols")

        # Store in XCom for downstream tasks
        context['task_instance'].xcom_push(key='symbols', value=symbols)

        return symbols

    except Exception as e:
        logger.error(f"Error getting active symbols: {e}")
        # Return a small default set if database query fails
        default_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        logger.warning(f"Using default symbols: {default_symbols}")
        return default_symbols


def precompute_static_features_task(**context):
    """
    Pre-compute static features for all symbols.
    
    Args:
        context: Airflow context
    """
    # Get symbols from previous task
    symbols = context['task_instance'].xcom_pull(
        task_ids='get_active_symbols',
        key='symbols'
    )
    
    if not symbols:
        logger.warning("No symbols provided for pre-computation")
        return
    
    # Calculate target date (next trading day)
    execution_date = context['execution_date']
    target_date = execution_date + timedelta(days=1)
    
    # Skip weekends
    while target_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        target_date += timedelta(days=1)
    
    logger.info(f"Pre-computing static features for {len(symbols)} symbols on {target_date.date()}")
    
    try:
        # Initialize ML dataset service
        ml_service = MLDatasetService()
        
        # Pre-compute static features
        stats = ml_service.precompute_features(
            symbols=symbols,
            target_date=target_date.date(),
            feature_types=['static']
        )
        
        logger.info(f"Static feature pre-computation completed: {stats}")
        
        # Store results in XCom
        context['task_instance'].xcom_push(
            key='static_precompute_stats',
            value=stats
        )
        
        ml_service.close()
        
        return stats
        
    except Exception as e:
        logger.error(f"Error pre-computing static features: {e}")
        raise


def precompute_semi_static_features_task(**context):
    """
    Pre-compute semi-static features for all symbols.
    
    Args:
        context: Airflow context
    """
    # Get symbols from previous task
    symbols = context['task_instance'].xcom_pull(
        task_ids='get_active_symbols',
        key='symbols'
    )
    
    if not symbols:
        logger.warning("No symbols provided for semi-static pre-computation")
        return
    
    # Calculate target date (next trading day)
    execution_date = context['execution_date']
    target_date = execution_date + timedelta(days=1)
    
    # Skip weekends
    while target_date.weekday() >= 5:
        target_date += timedelta(days=1)
    
    logger.info(f"Pre-computing semi-static features for {len(symbols)} symbols on {target_date.date()}")
    
    try:
        # Initialize ML dataset service
        ml_service = MLDatasetService()
        
        # Pre-compute semi-static features
        stats = ml_service.precompute_features(
            symbols=symbols,
            target_date=target_date.date(),
            feature_types=['semi_static']
        )
        
        logger.info(f"Semi-static feature pre-computation completed: {stats}")
        
        # Store results in XCom
        context['task_instance'].xcom_push(
            key='semi_static_precompute_stats',
            value=stats
        )
        
        ml_service.close()
        
        return stats
        
    except Exception as e:
        logger.error(f"Error pre-computing semi-static features: {e}")
        raise


def validate_precomputation_task(**context):
    """
    Validate that pre-computation completed successfully.
    
    Args:
        context: Airflow context
    """
    # Get statistics from previous tasks
    static_stats = context['task_instance'].xcom_pull(
        task_ids='precompute_static_features',
        key='static_precompute_stats'
    )
    
    semi_static_stats = context['task_instance'].xcom_pull(
        task_ids='precompute_semi_static_features', 
        key='semi_static_precompute_stats'
    )
    
    symbols = context['task_instance'].xcom_pull(
        task_ids='get_active_symbols',
        key='symbols'
    )
    
    logger.info("Validating pre-computation results")
    
    validation_result = {
        'symbols_requested': len(symbols) if symbols else 0,
        'static_symbols_processed': static_stats.get('symbols_processed', 0) if static_stats else 0,
        'static_features_computed': static_stats.get('features_computed', 0) if static_stats else 0,
        'semi_static_symbols_processed': semi_static_stats.get('symbols_processed', 0) if semi_static_stats else 0,
        'semi_static_features_computed': semi_static_stats.get('features_computed', 0) if semi_static_stats else 0,
        'status': 'unknown'
    }
    
    # Determine validation status
    expected_symbols = validation_result['symbols_requested']
    static_processed = validation_result['static_symbols_processed']
    semi_static_processed = validation_result['semi_static_symbols_processed']
    
    if static_processed >= expected_symbols * 0.9 and semi_static_processed >= expected_symbols * 0.9:
        validation_result['status'] = 'success'
    elif static_processed >= expected_symbols * 0.7 and semi_static_processed >= expected_symbols * 0.7:
        validation_result['status'] = 'partial'
    else:
        validation_result['status'] = 'failed'
    
    logger.info(f"Pre-computation validation: {validation_result}")
    
    # Store validation results
    context['task_instance'].xcom_push(
        key='precompute_validation',
        value=validation_result
    )
    
    # Fail task if validation failed
    if validation_result['status'] == 'failed':
        raise ValueError(f"Pre-computation validation failed: {validation_result}")
    
    return validation_result


def cleanup_precompute_cache_task(**context):
    """
    Clean up any temporary cache from pre-computation.
    
    Args:
        context: Airflow context
    """
    logger.info("Cleaning up pre-computation cache")
    
    try:
        # Initialize ML dataset service to access feature computer
        ml_service = MLDatasetService()
        
        # Reset computation statistics
        ml_service.feature_computer.reset_stats()
        
        # Clear temporal data access cache
        ml_service.feature_computer.data_access.clear_cache()
        
        ml_service.close()
        
        logger.info("Pre-computation cache cleanup completed")
        
    except Exception as e:
        logger.error(f"Error cleaning up pre-computation cache: {e}")
        # Don't fail the DAG for cleanup errors
        pass


def generate_precompute_report_task(**context):
    """
    Generate a summary report of pre-computation results.
    
    Args:
        context: Airflow context
    """
    # Get all results from previous tasks
    symbols = context['task_instance'].xcom_pull(
        task_ids='get_active_symbols',
        key='symbols'
    )
    
    static_stats = context['task_instance'].xcom_pull(
        task_ids='precompute_static_features',
        key='static_precompute_stats'
    )
    
    semi_static_stats = context['task_instance'].xcom_pull(
        task_ids='precompute_semi_static_features',
        key='semi_static_precompute_stats'
    )
    
    validation_result = context['task_instance'].xcom_pull(
        task_ids='validate_precomputation',
        key='precompute_validation'
    )
    
    execution_date = context['execution_date']
    target_date = execution_date + timedelta(days=1)
    
    # Generate report
    report = f"""
    ML Feature Pre-computation Report
    
    Execution Date: {execution_date.date()}
    Target Date: {target_date.date()}
    
    Symbols Processing:
    - Total Symbols: {len(symbols) if symbols else 0}
    - Static Features Processed: {static_stats.get('symbols_processed', 0) if static_stats else 0}
    - Semi-Static Features Processed: {semi_static_stats.get('symbols_processed', 0) if semi_static_stats else 0}
    
    Features Computed:
    - Static Features: {static_stats.get('features_computed', 0) if static_stats else 0}
    - Semi-Static Features: {semi_static_stats.get('features_computed', 0) if semi_static_stats else 0}
    - Total Features: {(static_stats.get('features_computed', 0) if static_stats else 0) + (semi_static_stats.get('features_computed', 0) if semi_static_stats else 0)}
    
    Validation Status: {validation_result.get('status', 'unknown') if validation_result else 'unknown'}
    
    Feature Types Pre-computed:
    - Static: Company sector, industry, employee count, country, exchange
    - Semi-Static: Market cap, market cap category, shares outstanding
    
    Next Steps:
    - Features are now available in Redis for fast access
    - Dynamic features will be computed on-demand during trading
    - Daily sync will move features to PostgreSQL for permanent storage
    """
    
    logger.info(f"Pre-computation report: {report}")
    
    return report


# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Daily pre-computation of static and semi-static ML features',
    schedule_interval=SCHEDULE_INTERVAL,
    tags=['ml', 'features', 'precompute', 'daily'],
    catchup=False
)

# Define tasks
get_active_symbols = PythonOperator(
    task_id='get_active_symbols',
    python_callable=get_active_symbols_task,
    dag=dag,
    doc_md="""
    ## Get Active Symbols Task
    
    Retrieves the list of active trading symbols from the master symbol list.
    This determines which symbols will have features pre-computed.
    """
)

precompute_static_features = PythonOperator(
    task_id='precompute_static_features',
    python_callable=precompute_static_features_task,
    dag=dag,
    doc_md="""
    ## Pre-compute Static Features Task
    
    Pre-computes static features that rarely change:
    - Company sector and industry
    - Employee count and country
    - Exchange and currency information
    """
)

precompute_semi_static_features = PythonOperator(
    task_id='precompute_semi_static_features',
    python_callable=precompute_semi_static_features_task,
    dag=dag,
    doc_md="""
    ## Pre-compute Semi-Static Features Task
    
    Pre-computes semi-static features that change periodically:
    - Market capitalization and category
    - Shares outstanding and float
    """
)

validate_precomputation = PythonOperator(
    task_id='validate_precomputation', 
    python_callable=validate_precomputation_task,
    dag=dag,
    doc_md="""
    ## Validate Pre-computation Task
    
    Validates that pre-computation completed successfully for the expected
    number of symbols and features.
    """
)

cleanup_precompute_cache = PythonOperator(
    task_id='cleanup_precompute_cache',
    python_callable=cleanup_precompute_cache_task,
    dag=dag,
    doc_md="""
    ## Cleanup Pre-computation Cache Task
    
    Cleans up temporary caches and resets statistics from the 
    pre-computation process.
    """
)

generate_precompute_report = PythonOperator(
    task_id='generate_precompute_report',
    python_callable=generate_precompute_report_task,
    dag=dag,
    trigger_rule='all_success',
    doc_md="""
    ## Generate Pre-computation Report Task
    
    Generates a summary report of pre-computation results including
    statistics and validation status.
    """
)

# Define task dependencies
get_active_symbols >> [precompute_static_features, precompute_semi_static_features]
[precompute_static_features, precompute_semi_static_features] >> validate_precomputation
validate_precomputation >> cleanup_precompute_cache >> generate_precompute_report

# Add documentation
dag.doc_md = """
# ML Feature Pre-computation DAG

This DAG pre-computes static and semi-static features to improve performance
during live trading. By computing these features ahead of time, the system
can focus on dynamic feature computation during market hours.

## Schedule
- **Frequency**: Daily at 6:00 PM ET
- **Days**: Monday through Friday (trading days only)
- **Timing**: Runs before the sync DAG to prepare features

## Feature Types

### Static Features (Rarely Change)
- Company sector, industry, country
- Employee count, exchange, currency
- Business summary length
- ETF classification

### Semi-Static Features (Change Periodically) 
- Market capitalization and category
- Shares outstanding and float shares
- Boolean sector indicators
- Market cap category indicators

## Performance Benefits
- Reduces computation time during live trading
- Enables faster model inference (<10ms target)
- Improves system reliability under market pressure

## Architecture Integration
- Pre-computed features are stored in Redis
- Available immediately for live trading
- Synced to PostgreSQL by the daily sync DAG

For more details, see the ML Featured Dataset documentation.
"""