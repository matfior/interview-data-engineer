#!/usr/bin/env python3
"""
Sales Data Ingestion Script

This script processes the sales data CSV file, handles data quality issues,
and loads the data into a PostgreSQL database.

It can be run with a specific date parameter to only process data for that date,
making it idempotent when run multiple times.
"""

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, MetaData, Table, inspect, text
from datetime import datetime
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection parameters
DB_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432')
DB_USER = os.environ.get('POSTGRES_USER', 'postgres')
DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'mysecretpassword')
DB_NAME = os.environ.get('POSTGRES_DB', 'sales')

# Data file path
DATA_FILE = '/data/generated_sales_data.csv'

def create_db_engine():
    """Create and return a SQLAlchemy database engine."""
    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def read_and_clean_data(file_path, target_date=None):
    """
    Read the CSV file and perform basic data cleaning.
    
    Args:
        file_path: Path to the CSV file
        target_date: Optional date string (YYYY-MM-DD) to filter data
        
    Returns:
        Cleaned pandas DataFrame
    """
    logger.info(f"Reading data from {file_path}")
    
    # Read the CSV file
    df = pd.read_csv(file_path)
    
    # Basic data cleaning
    logger.info("Performing data cleaning")
    
    # Handle missing values
    df['Location'] = df['Location'].fillna('Unknown')
    
    # Convert date string to datetime - handle both YYYY-MM-DD and YYYY/MM/DD formats
    df['Date'] = pd.to_datetime(df['Date'], format='mixed')
    
    # Filter by target date if provided
    if target_date:
        target_date = pd.to_datetime(target_date)
        logger.info(f"Filtering data for date: {target_date.strftime('%Y-%m-%d')}")
        df = df[df['Date'].dt.date == target_date.date()]
        logger.info(f"Found {len(df)} records for the target date")
        
        # If no data for target date, return empty DataFrame
        if len(df) == 0:
            logger.warning(f"No data found for date {target_date.strftime('%Y-%m-%d')}")
            return df
    
    # Remove duplicates
    original_count = len(df)
    df = df.drop_duplicates()
    duplicate_count = original_count - len(df)
    logger.info(f"Removed {duplicate_count} duplicate records")
    
    # Validate numeric columns
    numeric_cols = ['Quantity', 'Price']
    for col in numeric_cols:
        # Replace non-numeric values with NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')
        # Fill NaN with appropriate values (0 for Quantity, median for Price)
        if col == 'Quantity':
            df[col] = df[col].fillna(0).astype(int)
            # Handle negative quantities by taking absolute value
            df[col] = df[col].abs()
        else:
            median_price = df[col].median()
            df[col] = df[col].fillna(median_price)
    
    # Add a load timestamp column
    df['LoadTimestamp'] = datetime.now()
    
    return df

def create_tables_if_not_exist(engine):
    """
    Create the necessary tables in the database if they don't exist.
    
    Args:
        engine: SQLAlchemy engine
    """
    metadata = MetaData()
    
    # Define the sales table
    sales_table = Table(
        'raw_sales',
        metadata,
        Column('SaleID', Integer, primary_key=True),
        Column('ProductID', Integer),
        Column('ProductName', String(255)),
        Column('Brand', String(255)),
        Column('Category', String(255)),
        Column('RetailerID', Integer),
        Column('RetailerName', String(255)),
        Column('Channel', String(50)),
        Column('Location', String(255)),
        Column('Quantity', Integer),
        Column('Price', Float),
        Column('Date', Date),
        Column('LoadTimestamp', Date)
    )
    
    # Create tables
    inspector = inspect(engine)
    if not inspector.has_table('raw_sales'):
        logger.info("Creating raw_sales table")
        metadata.create_all(engine)
    else:
        logger.info("Table raw_sales already exists")

def delete_data_for_date(engine, date_str):
    """
    Delete data for a specific date from the raw_sales table.
    
    Args:
        engine: SQLAlchemy engine
        date_str: Date string in YYYY-MM-DD format
    """
    try:
        # Use raw SQL execution with autocommit=True to avoid transaction issues
        with engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(text(f"DELETE FROM raw_sales WHERE \"Date\" = '{date_str}'"))
        logger.info(f"Deleted existing data for date {date_str}")
        return True
    except Exception as e:
        logger.warning(f"Could not delete data for date {date_str}: {str(e)}")
        return False

def load_data_to_db(df, engine, table_name='raw_sales', if_exists='append', target_date=None):
    """
    Load the DataFrame to the database.
    
    Args:
        df: Pandas DataFrame to load
        engine: SQLAlchemy engine
        table_name: Name of the table to load data into
        if_exists: How to behave if the table already exists
        target_date: Optional date string to delete existing data for that date
    """
    if len(df) == 0:
        logger.info("No data to load")
        return
        
    logger.info(f"Loading {len(df)} records to {table_name}")
    
    # If we have a target date, delete existing data for that date
    if target_date and if_exists == 'append':
        delete_data_for_date(engine, target_date)
    
    # If we want to replace all data, first try to truncate the table
    if if_exists == 'replace':
        try:
            with engine.connect().execution_options(autocommit=True) as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            logger.info(f"Truncated table {table_name}")
        except Exception as e:
            logger.warning(f"Could not truncate table {table_name}: {str(e)}")
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        chunksize=1000
    )
    logger.info("Data loading completed")

def main():
    """Main function to orchestrate the ETL process."""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Process sales data')
        parser.add_argument('--date', type=str, help='Process data for this date (YYYY-MM-DD)')
        parser.add_argument('--replace', action='store_true', help='Replace all data instead of appending')
        args = parser.parse_args()
        
        target_date = args.date
        if_exists = 'replace' if args.replace else 'append'
        
        # Create database engine
        engine = create_db_engine()
        
        # Create tables if they don't exist
        create_tables_if_not_exist(engine)
        
        # Read and clean data
        df = read_and_clean_data(DATA_FILE, target_date)
        
        # Load data to database
        load_data_to_db(df, engine, if_exists=if_exists, target_date=target_date)
        
        logger.info("ETL process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 