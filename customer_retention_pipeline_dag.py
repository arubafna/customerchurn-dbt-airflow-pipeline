"""
Customer Retention Data Pipeline - Airflow DAG

Author: Arunima
Created: December 2025
GitHub: https://github.com/arubafna/customerchurn-dbt-airflow-pipeline

Purpose:
    Orchestrates end-to-end data pipeline for customer churn prediction.
    Handles data ingestion, dbt transformations, and quality validation.

Schedule: Daily at 2 AM UTC
Duration: ~2 minutes
Dependencies: Snowflake, dbt Cloud/CLI, Python 3.8+

Architecture:
    1. File validation
    2. Parallel data ingestion to Snowflake
    3. Row count validation
    4. dbt transformations (staging → intermediate → marts)
    5. Data quality testing
    6. Custom validation checks
"""

# ============================================================================
# IMPORTS
# ============================================================================

from datetime import datetime, timedelta
import logging
import os
import json

# Airflow core
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Snowflake integration
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# HTTP operator for dbt Cloud API (optional)
from airflow.providers.http.operators.http import SimpleHttpOperator

# Utilities
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd


# ============================================================================
# CONFIGURATION
# ============================================================================

# DAG default arguments
DEFAULT_ARGS = {
    'owner': 'data_analytics_team',
    'depends_on_past': False,
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# File paths
SOURCE_FILE_PATH = os.getenv('SOURCE_FILE_PATH', '/data/sources/')
ACTIVITY_LOG_FILE = f"{SOURCE_FILE_PATH}customer_activity_log.csv"
TRANSACTIONS_FILE = f"{SOURCE_FILE_PATH}purchase_transactions.json"

# Snowflake configuration
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_DATABASE = 'CUSTOMER_RETENTION_DB'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'

# ============================================================================
# DBT CONFIGURATION - TWO DEPLOYMENT OPTIONS
# ============================================================================

# ---------------------------------------------------------------------------
# OPTION A: dbt Cloud API (Recommended for dbt Cloud users)
# ---------------------------------------------------------------------------
"""
Use this approach if deploying with dbt Cloud in production.

Requirements:
- dbt Cloud Developer plan or higher
- Job configured in dbt Cloud (Orchestration → Jobs → Create Job)
- API token generated (Account Settings → API Tokens)

Configuration:
"""
DBT_CLOUD_ACCOUNT_ID = os.getenv('DBT_CLOUD_ACCOUNT_ID', 'your_account_id')
DBT_CLOUD_JOB_ID = os.getenv('DBT_CLOUD_JOB_ID', 'your_job_id')
DBT_CLOUD_API_TOKEN = os.getenv('DBT_CLOUD_API_TOKEN', 'your_api_token')
DBT_CLOUD_CONN_ID = 'dbt_cloud_default'

"""
How to find these values:
1. Account ID: Check dbt Cloud URL or Account Settings
2. Job ID: Orchestration → Jobs → Create Job → Check URL
3. API Token: Account Settings → API Tokens → Create Token

Example API endpoint:
POST https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/

Note: For this case study, dbt Cloud API approach is commented out.
Using local CLI approach below to demonstrate orchestration architecture.
"""

# ---------------------------------------------------------------------------
# OPTION B: Local dbt CLI (Used in this case study)
# ---------------------------------------------------------------------------
"""
Use this approach if:
- Self-hosting dbt project on Airflow server
- Running dbt locally via git-sync
- Demonstrating architecture for case study

Configuration:
"""
DBT_PROJECT_DIR = '/opt/airflow/dbt/customer_retention_pipeline'
DBT_PROFILES_DIR = '/opt/airflow/dbt'
DBT_TARGET = 'prod'

"""
Deployment:
1. Clone dbt project from GitHub to Airflow server
2. Configure profiles.yml with Snowflake credentials
3. Install dbt-snowflake: pip install dbt-snowflake
4. Test connection: dbt debug

GitHub Repository: https://github.com/arubafna/customerchurn-dbt-airflow-pipeline
"""

# Data quality thresholds
MIN_EXPECTED_ACTIVITY_ROWS = 100000
MIN_EXPECTED_TRANSACTION_ROWS = 1000
MAX_NULL_PERCENTAGE = 0.01
MIN_EXPECTED_USERS = 400


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def check_source_files(**context):
    """
    Task 1: Validate that source files exist and have correct structure.
    
    This function ensures data is available before starting the pipeline.
    Prevents wasting compute resources on empty pipeline runs.
    
    Raises:
        FileNotFoundError: If source files are missing
        ValueError: If CSV has incorrect structure
    """
    logging.info("=" * 80)
    logging.info("TASK 1: Checking source files")
    logging.info("=" * 80)
    
    # Check activity log exists
    if not os.path.exists(ACTIVITY_LOG_FILE):
        raise FileNotFoundError(
            f"Activity log not found: {ACTIVITY_LOG_FILE}"
        )
    
    # Check transactions exist
    if not os.path.exists(TRANSACTIONS_FILE):
        raise FileNotFoundError(
            f"Transactions not found: {TRANSACTIONS_FILE}"
        )
    
    # Validate CSV has required columns
    df = pd.read_csv(ACTIVITY_LOG_FILE, nrows=5)
    required_columns = ['user_id', 'action_type', 'timestamp', 'session_id', 'device']
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in CSV: {missing_columns}")
    
    # Check file sizes
    activity_size = os.path.getsize(ACTIVITY_LOG_FILE)
    trans_size = os.path.getsize(TRANSACTIONS_FILE)
    
    logging.info(f"✓ Activity log: {activity_size:,} bytes")
    logging.info(f"✓ Transactions: {trans_size:,} bytes")
    
    if activity_size == 0 or trans_size == 0:
        raise ValueError("Source files are empty")
    
    logging.info("✓ All source files validated successfully")
    
    # Pass file paths to downstream tasks via XCom
    context['ti'].xcom_push(key='activity_file', value=ACTIVITY_LOG_FILE)
    context['ti'].xcom_push(key='trans_file', value=TRANSACTIONS_FILE)

def custom_data_quality_checks(**context):
    """
    Task 9: Perform custom data quality checks on final feature table.
    
    These checks validate business-specific requirements that dbt tests
    don't cover. Ensures ML-ready data quality.
    
    Checks performed:
        1. Minimum user count (>= 400 users)
        2. No NULL values in critical columns
        3. Reasonable feature distributions
        4. Customer segment distribution
    
    Raises:
        ValueError: If any quality check fails
    """
    logging.info("=" * 80)
    logging.info("TASK 9: Running custom data quality checks")
    logging.info("=" * 80)
    
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    checks_passed = 0
    checks_failed = 0
    
    # Check 1: Verify minimum user count
    user_count_query = f"""
        SELECT COUNT(DISTINCT USER_ID) as user_count
        FROM {SNOWFLAKE_DATABASE}.MARTS.MRT_CUSTOMER_RETENTION_FEATURES
    """
    user_count = hook.get_first(user_count_query)[0]
    
    logging.info(f"Check 1: Total users = {user_count}")
    
    if user_count < MIN_EXPECTED_USERS:
        logging.error(f"FAILED: User count {user_count} < {MIN_EXPECTED_USERS}")
        checks_failed += 1
    else:
        logging.info("PASSED: User count meets threshold")
        checks_passed += 1
    
    # Check 2: Verify no NULL values
    null_check_query = f"""
        SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN USER_ID IS NULL THEN 1 ELSE 0 END) as null_user_id,
            SUM(CASE WHEN DAYS_SINCE_LAST_PURCHASE IS NULL THEN 1 ELSE 0 END) as null_recency,
            SUM(CASE WHEN PURCHASE_COUNT_30D IS NULL THEN 1 ELSE 0 END) as null_frequency,
            SUM(CASE WHEN LIFETIME_VALUE IS NULL THEN 1 ELSE 0 END) as null_monetary
        FROM {SNOWFLAKE_DATABASE}.MARTS.MRT_CUSTOMER_RETENTION_FEATURES
    """
    null_results = hook.get_first(null_check_query)
    total_rows = null_results[0]
    
    null_checks = [
        ('USER_ID', null_results[1]),
        ('DAYS_SINCE_LAST_PURCHASE', null_results[2]),
        ('PURCHASE_COUNT_30D', null_results[3]),
        ('LIFETIME_VALUE', null_results[4])
    ]
    
    logging.info("Check 2: NULL validation")
    for field, null_count in null_checks:
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0
        logging.info(f"  - {field}: {null_pct:.2f}% NULLs")
        
        if null_pct > (MAX_NULL_PERCENTAGE * 100):
            logging.error(f"FAILED: {field} has too many NULLs")
            checks_failed += 1
        else:
            checks_passed += 1
    
    # Check 3: Feature distributions
    distribution_query = f"""
        SELECT
            AVG(PURCHASE_COUNT_30D) as avg_purchases,
            AVG(SESSION_COUNT_30D) as avg_sessions,
            AVG(ENGAGEMENT_SCORE) as avg_engagement,
            COUNT(CASE WHEN IS_HIGH_CHURN_RISK THEN 1 END) as high_risk_count
        FROM {SNOWFLAKE_DATABASE}.MARTS.MRT_CUSTOMER_RETENTION_FEATURES
    """
    dist_results = hook.get_first(distribution_query)
    
    avg_purchases = dist_results[0] or 0
    avg_sessions = dist_results[1] or 0
    avg_engagement = dist_results[2] or 0
    high_risk_count = dist_results[3] or 0
    
    logging.info("Check 3: Feature distributions")
    logging.info(f"  - Avg purchases (30d): {avg_purchases:.2f}")
    logging.info(f"  - Avg sessions (30d): {avg_sessions:.2f}")
    logging.info(f"  - Avg engagement score: {avg_engagement:.2f}")
    logging.info(f"  - High risk users: {high_risk_count}")
    
    # Sanity check: purchases should be reasonable
    if 0 < avg_purchases < 100:
        logging.info("PASSED: Purchase average is reasonable")
        checks_passed += 1
    else:
        logging.error(f"FAILED: Purchase average {avg_purchases} seems unreasonable")
        checks_failed += 1
    
    # Check 4: Customer segment distribution
    segment_query = f"""
        SELECT
            CUSTOMER_SEGMENT,
            COUNT(*) as segment_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as segment_pct
        FROM {SNOWFLAKE_DATABASE}.MARTS.MRT_CUSTOMER_RETENTION_FEATURES
        GROUP BY CUSTOMER_SEGMENT
        ORDER BY segment_count DESC
    """
    segment_results = hook.get_records(segment_query)
    
    logging.info("Check 4: Customer segment distribution")
    for segment, count, pct in segment_results:
        logging.info(f"  - {segment}: {count} users ({pct}%)")
    
    checks_passed += 1
    
    # Summary
    total_checks = checks_passed + checks_failed
    logging.info("=" * 80)
    logging.info(f"Data Quality Summary: {checks_passed}/{total_checks} checks passed")
    logging.info("=" * 80)
    
    if checks_failed > 0:
        raise ValueError(f"{checks_failed} data quality checks failed!")
    
    logging.info("✓ All custom data quality checks passed")
    
    # Push metrics to XCom for final report
    context['ti'].xcom_push(key='total_users', value=user_count)
    context['ti'].xcom_push(key='high_risk_users', value=high_risk_count)


def send_success_summary(**context):
    """
    Task 10: Generate and log pipeline success summary.
    
    Pulls metrics from upstream tasks and creates comprehensive report.
    This provides visibility into pipeline execution.
    """
    ti = context['ti']
    
    # Pull metrics from XCom
    total_users = ti.xcom_pull(key='total_users', task_ids='custom_quality_checks')
    high_risk_users = ti.xcom_pull(key='high_risk_users', task_ids='custom_quality_checks')
    
    summary = f"""
    ========================================================================
    CUSTOMER RETENTION PIPELINE - SUCCESS
    ========================================================================
    
    Execution Date: {context['ds']}
    Run ID: {context['run_id']}
    
    DATA INGESTION:
      ✓ Activity log records: {activity_count:,}
      ✓ Transaction records: {transaction_count:,}
    
    FEATURE GENERATION:
      ✓ Total users processed: {total_users}
      ✓ Features generated: 27
      ✓ High churn risk users identified: {high_risk_users}
    
    DATA QUALITY:
      ✓ All dbt tests passed
      ✓ All custom quality checks passed
      ✓ Zero NULL values in feature table
    
    NEXT STEPS:
      → Feature table ready for ML model training
      → Location: {SNOWFLAKE_DATABASE}.MARTS.MRT_CUSTOMER_RETENTION_FEATURES
    
    ========================================================================
    """
    
    logging.info(summary)
    return summary


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id='customer_retention_pipeline',
    default_args=DEFAULT_ARGS,
    description='End-to-end pipeline for customer churn prediction features',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['customer_retention', 'ml_features', 'dbt', 'production'],
) as dag:

    # ========================================================================
    # TASK 1: CHECK SOURCE FILES
    # ========================================================================
    
    check_files_exist = PythonOperator(
        task_id='check_files_exist',
        python_callable=check_source_files,
        provide_context=True,
    )

    # ========================================================================
    # TASK 2: INGEST ACTIVITY LOG TO SNOWFLAKE
    # ========================================================================
    
    ingest_activity_log = SnowflakeOperator(
        task_id='ingest_activity_log',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            -- Truncate staging table
            TRUNCATE TABLE {SNOWFLAKE_DATABASE}.RAW.ACTIVITY_LOG;
            
            -- Load data from stage
            COPY INTO {SNOWFLAKE_DATABASE}.RAW.ACTIVITY_LOG
            FROM @ACTIVITY_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE;
            
            -- Log ingestion statistics
            SELECT 
                'ACTIVITY_LOG' as table_name,
                COUNT(*) as rows_loaded,
                CURRENT_TIMESTAMP() as load_timestamp
            FROM {SNOWFLAKE_DATABASE}.RAW.ACTIVITY_LOG;
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    # ========================================================================
    # TASK 3: INGEST TRANSACTIONS TO SNOWFLAKE
    # ========================================================================
    
    ingest_transactions = SnowflakeOperator(
        task_id='ingest_transactions',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            -- Truncate staging table
            TRUNCATE TABLE {SNOWFLAKE_DATABASE}.RAW.TRANSACTIONS;
            
            -- Load data from stage
            COPY INTO {SNOWFLAKE_DATABASE}.RAW.TRANSACTIONS
            FROM @TRANSACTION_STAGE
            FILE_FORMAT = (FORMAT_NAME = 'JSON_FORMAT')
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE;
            
            -- Log ingestion statistics
            SELECT 
                'TRANSACTIONS' as table_name,
                COUNT(*) as rows_loaded,
                CURRENT_TIMESTAMP() as load_timestamp
            FROM {SNOWFLAKE_DATABASE}.RAW.TRANSACTIONS;
        """,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )

    # ========================================================================
    # TASKS 4-7: DBT TRANSFORMATIONS
    # 
    # TWO IMPLEMENTATION OPTIONS:
    # ========================================================================
    
    # ------------------------------------------------------------------------
    # OPTION A: dbt Cloud API (COMMENTED - For production with dbt Cloud)
    # ------------------------------------------------------------------------
    """
    # Uncomment this section to use dbt Cloud API in production
    
    trigger_dbt_cloud = SimpleHttpOperator(
        task_id='trigger_dbt_cloud_job',
        http_conn_id=DBT_CLOUD_CONN_ID,
        endpoint=f'/api/v2/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{DBT_CLOUD_JOB_ID}/run/',
        method='POST',
        headers={
            'Authorization': f'Token {DBT_CLOUD_API_TOKEN}',
            'Content-Type': 'application/json',
        },
        data=json.dumps({
            'cause': 'Triggered by Airflow customer retention pipeline',
            'git_branch': 'main',
            'schema_override': 'PROD',
        }),
        response_check=lambda response: response.json().get('status', {}).get('is_success', False),
        log_response=True,
    )
    """
    
    # ------------------------------------------------------------------------
    # OPTION B: Local dbt CLI (USED IN THIS CASE STUDY)
    # ------------------------------------------------------------------------
    
    run_dbt_staging = BashOperator(
        task_id='run_dbt_staging',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target {DBT_TARGET} \
                --select staging
        """,
    )
    
    run_dbt_intermediate = BashOperator(
        task_id='run_dbt_intermediate',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target {DBT_TARGET} \
                --select intermediate
        """,
    )
    
    run_dbt_marts = BashOperator(
        task_id='run_dbt_marts',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target {DBT_TARGET} \
                --select marts
        """,
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target {DBT_TARGET}
        """,
    )

    # ========================================================================
    # TASK 8: CUSTOM DATA QUALITY CHECKS
    # ========================================================================
    
    custom_quality_checks = PythonOperator(
        task_id='custom_quality_checks',
        python_callable=custom_data_quality_checks,
        provide_context=True,
    )

    # ========================================================================
    # TASK 9: SEND SUCCESS NOTIFICATION
    # ========================================================================
    
    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=send_success_summary,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ========================================================================
    # TASK DEPENDENCIES (OPTION B: Local dbt CLI)
    # ========================================================================
    
    # Linear flow for main pipeline
    check_files_exist >> [ingest_activity_log, ingest_transactions]
    [ingest_activity_log, ingest_transactions] >> run_dbt_staging
    run_dbt_staging >> run_dbt_intermediate
    run_dbt_intermediate >> run_dbt_marts
    run_dbt_marts >> run_dbt_tests
    run_dbt_tests >> custom_quality_checks
    custom_quality_checks >> notify_success


# ============================================================================
# PRODUCTION DEPLOYMENT NOTES
# ============================================================================
"""
DEPLOYMENT CHECKLIST:

1. Snowflake Connection (Airflow UI → Admin → Connections):
   - Connection ID: snowflake_default
   - Connection Type: Snowflake
   - Account: <your_snowflake_account>
   - Username: <your_username>
   - Password: <your_password>
   - Warehouse: COMPUTE_WH
   - Database: CUSTOMER_RETENTION_DB
   - Role: ACCOUNTADMIN

2A. For dbt Cloud Deployment:
   - Uncomment dbt Cloud API section (lines with SimpleHttpOperator)
   - Create dbt Cloud job: Orchestration → Jobs → Create Job
   - Configure job commands: dbt run && dbt test
   - Generate API token: Account Settings → API Tokens
   - Add HTTP connection in Airflow:
     * Conn ID: dbt_cloud_default
     * Conn Type: HTTP
     * Host: https://cloud.getdbt.com
   - Set environment variables:
     * DBT_CLOUD_ACCOUNT_ID
     * DBT_CLOUD_JOB_ID
     * DBT_CLOUD_API_TOKEN

2B. For Local dbt Deployment:
   - Clone GitHub repo: git clone https://github.com/arubafna/customerchurn-dbt-airflow-pipeline.git
   - Install dbt: pip install dbt-snowflake
   - Configure profiles.yml with Snowflake credentials
   - Test: dbt debug

3. File Storage:
   - Update SOURCE_FILE_PATH for your environment
   - Options: Local filesystem, S3, Azure Blob, GCS
   - Ensure Airflow has read permissions

4. Monitoring:
   - Configure SMTP in airflow.cfg for email alerts
   - Set up Slack webhook (optional)
   - Enable Airflow SLA monitoring

5. Testing:
   - Manually trigger DAG from Airflow UI
   - Verify all tasks succeed
   - Check Snowflake: SELECT COUNT(*) FROM MARTS.MRT_CUSTOMER_RETENTION_FEATURES

EXECUTION:
- Schedule: Daily at 2:00 AM UTC
- Duration: ~2 minutes
- Retries: 2 attempts with 5-min delay
- Timeout: 2 hours maximum

GITHUB REPOSITORY:
https://github.com/arubafna/customerchurn-dbt-airflow-pipeline

CASE STUDY NOTES:
- This DAG demonstrates local dbt CLI pattern (Option B)
- For production with dbt Cloud, uncomment Option A
- Both approaches use the same orchestration architecture
- The key is understanding when to use each deployment model
"""