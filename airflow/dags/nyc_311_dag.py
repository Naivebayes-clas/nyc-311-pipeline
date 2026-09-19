import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook   
import subprocess
import os

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def fetch_311_data(**context):
    """Step 1: Pull 3 months of data from NYC 311 API (one call per month)."""
    print("Fetching data from NYC 311 API...")
    all_data = []

    # Pull the last 3 months (each call gets up to 20K rows for that month)
    months_back = 6
    today = datetime.now()

    for i in range(months_back):
        # Calculate start/end of each month
        end_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1) + timedelta(days=1) - timedelta(days=i*30)
        start_month = end_month - timedelta(days=30)

        where_clause = f"created_date >= '{start_month.strftime('%Y-%m-%d')}T00:00:00' AND created_date < '{end_month.strftime('%Y-%m-%d')}T00:00:00'"

        url = f"{BASE_URL}?$where={where_clause}&$limit=10000"
        print(f"  Fetching {start_month.strftime('%Y-%m')} ... {end_month.strftime('%Y-%m')}")

        response = requests.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
        print(f"    Got {len(data)} rows")
        all_data.extend(data)

    print(f"Total fetched: {len(all_data)} records")

    df = pd.DataFrame(all_data)
    df.to_csv("/tmp/nyc_311_raw.csv", index=False)
    return len(all_data)

def load_to_postgres(**context):
    """Step 2: Clean and load into PostgreSQL."""
    df = pd.read_csv("/tmp/nyc_311_raw.csv")

    # Basic cleaning
    df["created_date"] = pd.to_datetime(df["created_date"])
    df["borough"] = df["borough"].str.strip().str.upper()
    df["complaint_type"] = df["complaint_type"].str.strip()
    df = df.dropna(subset=["borough", "complaint_type"])
    print(f"Cleaned: {len(df)} rows remain")

    pg_hook = PostgresHook(postgres_conn_id="nyc311_postgres")
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_311_requests (
            unique_key     BIGINT PRIMARY KEY,
            created_date   TIMESTAMP,
            borough           VARCHAR(20),
            complaint_type VARCHAR(200),
            status         VARCHAR(50),
            agency         VARCHAR(200),
            location_type  VARCHAR(50),
            latitude       DOUBLE PRECISION,
            longitude      DOUBLE PRECISION
        );
    """)
    cur.execute("TRUNCATE TABLE raw_311_requests;")

    records = df[["unique_key","created_date","borough","complaint_type",
                  "status","agency","location_type","latitude","longitude"]].values.tolist()
    cur.executemany("""
        INSERT INTO raw_311_requests VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (unique_key) DO NOTHING;
    """, records)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(records)} rows into Postgres")

def run_dbt(**context):
    """Step 3: Run dbt to build the analytics tables."""
    dbt_dir = os.path.expanduser("~/nyc-311-pipeline/dbt_project")
    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", dbt_dir],
        capture_output=True, text=True, cwd=dbt_dir
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt failed: {result.stderr or result.stdout}")
    print("dbt build completed successfully!")

default_args = {
    "owner": "you",
    "start_date": datetime(2025, 9, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_311_etl",
    default_args=default_args,
    schedule="@daily",   
    catchup=False,
    tags=["nyc311", "etl"],
) as dag:
    fetch = PythonOperator(task_id="fetch_311_data", python_callable=fetch_311_data)
    load  = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres)
    transform = PythonOperator(task_id="run_dbt", python_callable=run_dbt)

    fetch >> load >> transform
   
