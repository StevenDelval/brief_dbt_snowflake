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
DBT_PASSWORD = os.getenv("DBT_PASSWORD")

# Connexion Snowflake #
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
)
cursor = conn.cursor()

cursor.execute("USE ROLE ACCOUNTADMIN;")

## Créer le rôle TRANSFORM et lui donner les droits ##
cursor.execute("CREATE ROLE IF NOT EXISTS TRANSFORM;")
cursor.execute("GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;")

## Créer Warehouse si nécessaire ##
cursor.execute("""
CREATE WAREHOUSE IF NOT EXISTS NYC_TAXI_WH
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
""")
cursor.execute("GRANT OPERATE ON WAREHOUSE NYC_TAXI_WH TO ROLE TRANSFORM;")
cursor.execute("USE WAREHOUSE NYC_TAXI_WH;")

## Créer l'utilisateur DBT ##
cursor.execute(f"""
CREATE USER IF NOT EXISTS dbt
  PASSWORD='{DBT_PASSWORD}'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='NYC_TAXI_WH'
  DEFAULT_ROLE='TRANSFORM'
  DEFAULT_NAMESPACE='NYC_TAXI_DB.RAW'
  COMMENT='DBT user for data transformation';
""")
cursor.execute("GRANT ROLE TRANSFORM TO USER dbt;")

## Créer Database et Schemas si nécessaire ##
cursor.execute("CREATE DATABASE IF NOT EXISTS NYC_TAXI_DB;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.RAW;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.STAGING;")
cursor.execute("CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DB.FINAL;")

## Donner les permissions au rôle TRANSFORM ## 
cursor.execute("GRANT ALL ON WAREHOUSE NYC_TAXI_WH TO ROLE TRANSFORM;")
cursor.execute("GRANT ALL ON DATABASE NYC_TAXI_DB TO ROLE TRANSFORM;")
cursor.execute("GRANT ALL ON ALL SCHEMAS IN DATABASE NYC_TAXI_DB TO ROLE TRANSFORM;")
cursor.execute("GRANT ALL ON FUTURE SCHEMAS IN DATABASE NYC_TAXI_DB TO ROLE TRANSFORM;")
cursor.execute("GRANT ALL ON ALL TABLES IN SCHEMA NYC_TAXI_DB.RAW TO ROLE TRANSFORM;")
cursor.execute("GRANT ALL ON FUTURE TABLES IN SCHEMA NYC_TAXI_DB.RAW TO ROLE TRANSFORM;")

## Créer table RAW ##
cursor.execute("USE DATABASE NYC_TAXI_DB;")
cursor.execute("USE SCHEMA RAW;")
cursor.execute("""
CREATE IF NOT EXISTS TABLE  yellow_tripdata (
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

## Charge les données dans snowflake ##
def load_file(year: int, month: int) -> None:
    """
    Télécharge un fichier Parquet Yellow Taxi depuis CloudFront pour un mois donné,
    transforme les colonnes nécessaires et charge les données dans Snowflake.

    Étapes réalisées :
    1. Construire l'URL du fichier Parquet à partir de l'année et du mois spécifiés.
    2. Télécharger et lire le fichier dans un DataFrame Pandas.
    3. Convertir les colonnes de date/heure (`tpep_pickup_datetime` et `tpep_dropoff_datetime`)
    au format chaîne de caractères 'YYYY-MM-DD HH:MM:SS'.
    4. Ajouter une colonne `file_name` contenant le nom du fichier source.
    5. Convertir tous les noms de colonnes en majuscules pour correspondre à la table Snowflake.
    6. Insérer le DataFrame dans la table `RAW.YELLOW_TRIPDATA` via `write_pandas`.
    7. Afficher le nombre de lignes insérées.

    Paramètres
    ----------
    year : int
        L'année du fichier à télécharger (ex. : 2024, 2025).
    month : int
        Le mois du fichier à télécharger (1-12).
    """

    # Construire l'URL du fichier Parquet pour l'année et le mois donnés
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    print(f"Téléchargement et chargement : {url}")
    
    # Lire le fichier Parquet dans un DataFrame Pandas
    df = pd.read_parquet(url)

    # Convertir les colonnes datetime en chaînes de caractères pour Snowflake
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Ajouter une colonne pour le nom du fichier source
    df['file_name'] = f"yellow_tripdata_{year}-{month:02d}.parquet"

    # Uniformiser les noms de colonnes en majuscules pour correspondre à la table Snowflake
    df.columns = [c.upper() for c in df.columns]

    # Insérer le DataFrame dans la table RAW.YELLOW_TRIPDATA via write_pandas
    success, nchunks, nrows, _ = write_pandas(conn, df, "YELLOW_TRIPDATA", schema="RAW")

    print(f" → {nrows} lignes insérées")

# Boucle sur toutes les années et tous les mois souhaités
for year in [2024, 2025]:
    for month in range(1, 13):
        try:
            # Charger chaque fichier individuellement
            load_file(year, month)
        except Exception as e:
            print(f"Erreur lors du chargement de {year}-{month:02d} : {e}")

## Fermer le curseur et la connexion Snowflake ##
cursor.close()
conn.close()