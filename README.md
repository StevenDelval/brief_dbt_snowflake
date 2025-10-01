# brief_dbt_snowflake
## NYC Taxi Data Loader

This Python project automates the loading of New York City Yellow Taxi trip data into a Snowflake database. It downloads Parquet files from CloudFront, transforms them, and inserts them into Snowflake for downstream analysis.

### Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Features](#features)

#### Prerequisites

Python 3.10 or higher
Snowflake account with permissions to create:
- Warehouses
- Databases
- Schemas
- Tables

#### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/your-username/nyc-taxi-loader.git
    cd nyc-taxi-loader
    ```
2. (Optional) Create a virtual environment:
    ```python
    python -m venv venv
    source venv/bin/activate  # Linux/Mac
    venv\Scripts\activate
    ```
3. Install dependencies:
    ```python 
    pip install -r requirements.txt
    ```

#### Configuration
1. Create a .env file in the project root with your Snowflake credentials:
    ```env
    SNOWFLAKE_USER=<your_user>
    SNOWFLAKE_PASSWORD=<your_password>
    SNOWFLAKE_ACCOUNT=<your_account>
    ```
2. Ensure your Snowflake account has the required privileges to create warehouses, databases, schemas, and tables.


#### Usage
Run the main script to load data:
```bash
python init_snowflake.py
```

The script performs the following steps:

1. Connects to Snowflake
2. Creates the warehouse NYC_TAXI_WH if it doesn't exist
3. Creates the database NYC_TAXI_DB and schemas RAW, STAGING, and FINAL
4. Creates the table RAW.YELLOW_TRIPDATA
5. Downloads and loads Parquet files for 2024 and 2025
6. Prints the number of rows inserted for each file

Project Structure
```
nyc-taxi-loader/
│
├── init_snowflake.py      # Main data loading script
├── requirements.txt       # Python dependencies
├── .env.example           # Example environment variables file
└── README.md              # Project documentation
```

#### Features

- Automatic download of NYC Yellow Taxi Parquet files from CloudFront
- Datetime transformation to Snowflake-compatible format
- Adds source file name column
- Loads data into Snowflake using write_pandas
- Handles errors for individual files