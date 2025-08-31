from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator


REPO_DIR = "/opt/airflow/dags/repo"

default_args = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# daily 08:00 Europe/Paris
with DAG(
    dag_id="youtube_dbt_daily",
    start_date=datetime(2025,1,1),
    schedule="0 8 * * *",
    catchup=False,
    default_args={"retries":1, "retry_delay": timedelta(minutes=5)},
    description="Ingest YouTube -> dbt seed/run/test",
) as dag:

    ingest = BashOperator(
        task_id="ingest_youtube",
        bash_command=(
            f'cd {REPO_DIR} && '
            'test -n "$YOUTUBE_API_KEY" || (echo "Missing YOUTUBE_API_KEY" && exit 1); '
            'python ingest_youtube.py '
            '--channel-id "$CHANNEL_ID" '
            '--max-results "${MAX_RESULTS:-25}" '
            '--out data/raw/youtube_videos.csv'
        ),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {REPO_DIR} && dbt seed --profiles-dir profiles",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {REPO_DIR} && dbt run --profiles-dir profiles",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {REPO_DIR} && dbt test --profiles-dir profiles",
    )

    ingest >> dbt_seed >> dbt_run >> dbt_test
