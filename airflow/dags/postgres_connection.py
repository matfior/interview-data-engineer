"""
PostgreSQL Connection Module

This module provides functions to set up and manage PostgreSQL connections for Airflow.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Connection
from airflow import settings
import logging

def create_postgres_connection():
    """
    Create a PostgreSQL connection in Airflow if it doesn't exist.
    """
    conn_id = "postgres_default"
    
    # Check if connection already exists
    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if connection is None:
        # Create a new connection
        connection = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host="postgres",
            login="postgres",
            password="mysecretpassword",
            schema="sales",
            port=5432
        )
        session.add(connection)
        session.commit()
        logging.info(f"Connection {conn_id} created")
    else:
        logging.info(f"Connection {conn_id} already exists")
    
    session.close()

def check_data_exists(execution_date, **kwargs):
    """
    Check if data for the execution date already exists in the fact_sales table.
    
    Args:
        execution_date: The execution date to check (can be a string or datetime object)
        
    Returns:
        Boolean indicating whether to skip the downstream tasks
    """
    # Format the execution date as YYYY-MM-DD
    if isinstance(execution_date, str):
        # If it's already a string, try to parse it
        try:
            from datetime import datetime
            date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
            date_str = execution_date
        except ValueError:
            # If parsing fails, use the string as is
            date_str = execution_date
    else:
        # If it's a datetime object, format it
        date_str = execution_date.strftime('%Y-%m-%d')
    
    logging.info(f"Checking if data exists for date: {date_str}")
    
    # Create a PostgreSQL hook
    hook = PostgresHook(postgres_conn_id="postgres_default")
    
    # Check if the fact_sales table exists
    table_exists_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'fact_sales'
    );
    """
    table_exists = hook.get_first(table_exists_query)[0]
    
    if not table_exists:
        logging.info("fact_sales table does not exist yet. Proceeding with data processing.")
        return False
    
    # Check if data for the execution date exists
    query = f"""
    SELECT COUNT(*) 
    FROM fact_sales 
    WHERE sale_date = '{date_str}'
    """
    
    result = hook.get_first(query)[0]
    
    if result > 0:
        logging.info(f"Data for {date_str} already exists in fact_sales. Skipping processing.")
        return True
    else:
        logging.info(f"No data found for {date_str} in fact_sales. Proceeding with data processing.")
        return False 