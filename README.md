# Senior Data Engineer Take-Home Assignment

## Overview

This project implements a complete data pipeline for processing sales data, including:
- Data ingestion and cleaning
- PostgreSQL database integration
- Airflow orchestration
- dbt transformations
- Data quality tests

## Architecture

The solution is fully containerized using Docker with these components:
- **PostgreSQL**: Database for storing raw and transformed data
- **Airflow**: Workflow orchestration tool
- **dbt**: Data transformation tool
- **Python**: Data processing scripts

## Directory Structure

```
.
├── airflow/
│   ├── dags/
│   │   └── sales_data_pipeline.py
|   |   └── postgres_connection.py
│   └── plugins/
├── data/
│   └── generated_sales_data.csv
├── dbt/
│   ├── models/
│   │   ├── marts/
│   │   │   ├── dim_product.sql
│   │   │   ├── fact_sales.sql
│   │   │   └── marts.yml
│   │   └── staging/
│   │       ├── stg_sales.sql
│   │       └── stg_sales.yml
│   ├── macros/
│   │   └── positive_values.sql
│   ├── profiles/
│   │   └── profiles.yml
│   └── dbt_project.yml
├── initdb/
│   └── init_sales_db.sql
├── scripts/
│   └── ingest_sales_data.py
├── docker-compose.yml
├── Dockerfile
└── requirements.txt
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose

### Running the Solution

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Start the Docker containers:
   ```bash
   docker-compose up -d
   ```

3. Access the services:
   - Airflow: http://localhost:8080 (username: admin, password: admin)
   - PostgreSQL: localhost:5432 (username: postgres, password: mysecretpassword)

## Pipeline Details

### 1. Data Ingestion

The data ingestion process:
- Reads the CSV file
- Handles missing values and duplicates
- Validates data types
- Loads the data into PostgreSQL

### 2. Database Integration

The database schema includes:
- Raw sales table with appropriate indexes
- Staging views created by dbt
- Dimensional models (fact and dimension tables)

### 3. Airflow Orchestration

The Airflow DAG orchestrates the following tasks:
- Data ingestion
- Index creation
- dbt transformations
- Data quality tests

### 4. dbt Implementation

The dbt project includes:
- Staging models that clean and prepare the data
- Dimensional models (fact_sales and dim_product)
- Data quality tests

## Data Quality

Data quality is ensured through:
- Data cleaning in the ingestion script
- dbt tests for uniqueness, not-null values, and relationships
- Custom tests for positive values

## Future Improvements

Potential improvements to the solution:
- Implement incremental loading for larger datasets
- Add more comprehensive data quality checks
- Create a dashboard for monitoring data quality
- Implement CI/CD for the data pipeline

## Troubleshooting

If you encounter any issues:
1. Check the Docker container logs:
   ```bash
   docker-compose logs -f
   ```
2. Verify that all services are running:
   ```bash
   docker-compose ps
   ```
3. Check the Airflow logs for specific task failures

👋 Welcome to the Hostaway Data Engineer Technical Test

## Objective
This assignment will test your skills in building an efficient data pipeline that processes CSV data, loads it into a database, orchestrates with Airflow, and transforms data using dbt.

## Requirements

### Part 1: Data Ingestion & Processing
- Create a script to ingest and parse the provided CSV file `./generated-sales-data.csv`
- Handle common data quality issues (missing values, duplicates)
- Design for incremental loading capabilities (optional)

### Part 2: Database Integration
- Load the processed data into PostgreSQL with appropriate schema design (docker compose provided)
- Implement basic indexing for performance optimization

### Part 3: Airflow Orchestration
- Create an Airflow DAG to orchestrate the pipeline
- Set up task dependencies that reflect the data flow

### Part 4: dbt Implementation
- Set up a dbt project to transform the raw data
- Create at least one staging model and one dimensional model
- Include basic tests for your models

### Part 5: Documentation
- Provide a README explaining your approach and setup instructions

## Provided Resources
- `generated-sales-data.csv`: Contains sales data
- Postgres docker compose 
- Connection details for PostgreSQL database

## Submission Guidelines
- Submit all code in a GitHub repository
- Complete the assignment within 5 days
- Be prepared to discuss your solution in a follow-up interview


### ---
### Start postgres
This creates a `sales` database
```bash
docker-compose up -d
```
*Note: if you have something running locally on port 5432 which will conflict with the postgres docker container then you can change the local port mapping in `docker-compose.yml` like so:*
```yaml
    ports:
      - "6543:5432"
``` 

### Good luck!