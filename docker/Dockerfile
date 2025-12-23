# Use official Airflow image as base
FROM apache/airflow:2.7.3

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        freetds-dev \
        git \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        postgresql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs
COPY dags/ /opt/airflow/dags/

# Copy SQL files
COPY sql/ /opt/airflow/sql/

# Copy scripts
COPY scripts/ /opt/airflow/scripts/