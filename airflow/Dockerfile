# Use official Airflow image as base
FROM apache/airflow:2.7.3-python3.10

# Install system tools (git) and cleanup
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Upgrade pip, setuptools, wheel
RUN pip install --no-cache-dir --upgrade "pip<24" "setuptools<70" wheel

# Install dbt with Snowflake adapter
RUN pip install --no-cache-dir "dbt-snowflake==1.4.5"

# Ensure dbt command is available globally inside container
ENV PATH="/home/airflow/.local/bin:${PATH}"
