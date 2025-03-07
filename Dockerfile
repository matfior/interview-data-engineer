FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt
RUN pip install --no-cache-dir dbt-postgres==1.5.0

# Create necessary directories
RUN mkdir -p /data /scripts /dbt

# Set environment variables
ENV PYTHONPATH=/app

# Copy application code
COPY . /app/

# Set the entrypoint
ENTRYPOINT ["/bin/bash"] 