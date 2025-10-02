from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="ETL NYC Taxi data + dbt transformations",
    schedule="@daily",  # <-- remplacé schedule_interval
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
) as dag:

    def run_python_etl():
        subprocess.run(["python", "/path/to/init_snowflake.py"], check=True)

    def run_dbt_build():
        subprocess.run(["dbt", "build", "--project-dir", "/path/to/nyc_taxi_dbt"], check=True)

    etl_task = PythonOperator(
        task_id="run_python_etl",
        python_callable=run_python_etl,
    )

    dbt_task = PythonOperator(
        task_id="run_dbt_build",
        python_callable=run_dbt_build,
    )

    etl_task >> dbt_task