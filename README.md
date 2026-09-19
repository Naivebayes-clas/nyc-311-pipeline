# --- README.md ---
cat > README.md << 'EOF'
# NYC 311 Complaints Pipeline

End-to-end data pipeline: pulls NYC 311 service requests from the Socrata API,
loads into PostgreSQL, transforms with dbt, and visualizes with Streamlit.

## Architecture
   
NYC 311 API → Airflow (ETL) → PostgreSQL → dbt (transform) → Streamlit (dashboard)


## Tech Stack

- Python 3.10+
- Airflow 2.10
- PostgreSQL 15 (Docker)
- dbt 1.8
- Pandas
- Streamlit

## Setup

```bash
# 1. Create virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Start PostgreSQL
docker compose up -d

# 3. Start Airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/airflow.db
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow users create --username admin --password admin \
  --firstname Dev --lastname User --role admin --email dev@example.com
airflow webserver -p 8080

# 4. Trigger the DAG
# Open http://localhost:8080 → nyc_311_etl → Trigger DAG

# 5. Start the dashboard
cd streamlit_app
streamlit run app.py   


What I Learned
- API ingestion vs. static CSV files
- dbt for modular, testable SQL transformations
- Docker for reproducible infrastructure
- Scheduling ETL with Airflow

