import io
import os
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Charger les variables d'environnement #
load_dotenv()
USER = os.getenv("SNOWFLAKE_USER")
PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# Connexion Snowflake #
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
)
cursor = conn.cursor()

cursor.execute("USE ROLE ACCOUNTADMIN;")
## Créer Warehouse si nécessaire ##
cursor.execute("""
CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
""")

## Créer Database et Schemas si nécessaire ##
cursor.execute("CREATE DATABASE IF NOT EXISTS NYC_TAXI_DB;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.RAW;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.STAGING;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.FINAL;")

## Créer table RAW ##
cursor.execute("USE DATABASE NYC_TAXI_DB;")
cursor.execute("USE SCHEMA RAW;")
cursor.execute("""
CREATE OR REPLACE TABLE  yellow_tripdata (
    vendorid INT,
    tpep_pickup_datetime DATETIME,
    tpep_dropoff_datetime DATETIME,
    passenger_count FLOAT,
    trip_distance FLOAT,
    ratecodeid FLOAT,
    store_and_fwd_flag STRING,
    pulocationid INT,
    dolocationid INT,
    payment_type INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT,
    file_name STRING
);
""")


def load_file(year, month):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    print(f"Téléchargement et chargement : {url}")
    
    df = pd.read_parquet(url)
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['file_name'] = f"yellow_tripdata_{year}-{month:02d}.parquet"

    df.columns = [c.upper() for c in df.columns]
    
    success, nchunks, nrows, _ = write_pandas(conn, df, "YELLOW_TRIPDATA", schema="RAW")
    print(f" → {nrows} lignes insérées")

for year in [2024, 2025]:
    for month in range(1, 13):
        try:
            load_file(year, month)
        except Exception as e:
            print(f"Erreur lors du chargement de {year}-{month:02d} : {e}")


cursor.close()
conn.close()