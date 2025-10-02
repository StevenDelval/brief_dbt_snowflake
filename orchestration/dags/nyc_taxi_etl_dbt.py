from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

DBT_PROJECT_DIR = "/usr/local/airflow/nyc_taxi_dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml est dans le projet

# --- Variables d'environnement Snowflake ---
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
DBT_PASSWORD = os.getenv("DBT_PASSWORD")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE= os.getenv("SNOWFLAKE_DATABASE")
SCHEMA= os.getenv("SNOWFLAKE_SCHEMA")

# --- Paramètres de fichiers à charger ---
YEARS = [2024, 2025]
MONTHS = range(1, 13)

YEARS = [2023,2024, 2025]
MONTHS = range(1, 13)

def load_missing_files():
    """Charge seulement les fichiers manquants dans Snowflake"""
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT
    )
    cursor = conn.cursor()
    cursor.execute(f"USE WAREHOUSE {WAREHOUSE};")
    cursor.execute(f"USE DATABASE {DATABASE};")
    cursor.execute(f"USE SCHEMA {SCHEMA};")

    all_files = [f"yellow_tripdata_{y}-{m:02d}.parquet" for y in YEARS for m in MONTHS]

    cursor.execute("SELECT DISTINCT FILE_NAME FROM RAW.YELLOW_TRIPDATA")
    loaded_files = set(row[0] for row in cursor.fetchall())

    for file_name in all_files:
        if file_name in loaded_files:
            print(f"{file_name} déjà chargé, passage au suivant...")
            continue

        year, month = map(int, file_name.replace("yellow_tripdata_", "").replace(".parquet", "").split("-"))
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        print(f"Téléchargement et chargement : {url}")

        try:
            df = pd.read_parquet(url)
            df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df['file_name'] = file_name
            df.columns = [c.upper() for c in df.columns]

            success, nchunks, nrows, _ = write_pandas(conn, df, "YELLOW_TRIPDATA", schema="RAW")
            print(f" → {nrows} lignes insérées")
        except Exception as e:
            print(f"Erreur lors du chargement de {file_name} : {e}")

    cursor.close()
    conn.close()


with DAG(
    dag_id="dbt_nyc_taxi_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    
    load_files_task = PythonOperator(
        task_id="load_missing_files",
        python_callable=load_missing_files
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    )

    load_files_task >> dbt_run >> dbt_test