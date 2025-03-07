"""
Sales Data Pipeline DAG

This DAG orchestrates the ETL process for sales data:
1. Ingest and process the sales data
2. Load the data into PostgreSQL
3. Run dbt transformations

This DAG is idempotent - it will check if data for the execution date already exists
and skip processing if it does.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# Import the connection setup function
import sys
sys.path.append('/opt/airflow/dags')
from postgres_connection import create_postgres_connection, check_data_exists

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['sales', 'etl'],
)

# Task 0: Set up PostgreSQL connection
setup_connection = PythonOperator(
    task_id='setup_postgres_connection',
    python_callable=create_postgres_connection,
    dag=dag,
)

# Task 0.5: Check if data for execution date already exists
check_existing_data = ShortCircuitOperator(
    task_id='check_existing_data',
    python_callable=check_data_exists,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

# Task 1: Ingest and process sales data
ingest_sales_data = BashOperator(
    task_id='ingest_sales_data',
    bash_command='python /scripts/ingest_sales_data.py --date {{ ds }}',
    dag=dag,
)

# Task 2: Create indexes for performance optimization
create_indexes = PostgresOperator(
    task_id='create_indexes',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE INDEX IF NOT EXISTS idx_raw_sales_date ON raw_sales("Date");
    CREATE INDEX IF NOT EXISTS idx_raw_sales_product ON raw_sales("ProductID");
    CREATE INDEX IF NOT EXISTS idx_raw_sales_retailer ON raw_sales("RetailerID");
    CREATE INDEX IF NOT EXISTS idx_raw_sales_category ON raw_sales("Category");
    """,
    dag=dag,
)

# Task 3: Run dbt to transform the data
run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /dbt && dbt run --profiles-dir /dbt/profiles --full-refresh',
    dag=dag,
)

# Task 4: Run dbt tests
test_dbt = BashOperator(
    task_id='test_dbt',
    bash_command='cd /dbt && dbt test --profiles-dir /dbt/profiles',
    dag=dag,
)

# Define task dependencies
setup_connection >> check_existing_data >> ingest_sales_data >> create_indexes >> run_dbt >> test_dbt 