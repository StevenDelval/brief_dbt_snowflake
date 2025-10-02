from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess

def run_python_etl():
    subprocess.run(["python", "/path/to/init_snowflake.py"], check=True)

def run_dbt_build():
    subprocess.run(["dbt", "build", "--project-dir", "/path/to/nyc_taxi_dbt"], check=True)

with DAG(
    "nyc_taxi_pipeline",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    description="ETL NYC Taxi data + dbt transformations",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id="run_python_etl",
        python_callable=run_python_etl,
    )

    dbt_task = PythonOperator(
        task_id="run_dbt_build",
        python_callable=run_dbt_build,
    )

    etl_task >> dbt_task