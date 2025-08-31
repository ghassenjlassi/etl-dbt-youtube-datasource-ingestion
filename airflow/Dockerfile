FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
USER airflow


RUN pip install --no-cache-dir \
    dbt-core==1.7.14 \
    dbt-duckdb==1.7.2 \
    duckdb==1.0.0 \
    google-api-python-client \
    pandas \
    python-dotenv \
    requests
