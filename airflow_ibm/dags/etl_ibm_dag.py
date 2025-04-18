from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

# Caminho absoluto para a pasta /usr/local/airflow/dags/src
SRC_DIR = os.path.join(os.path.dirname(__file__), "src")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='ibm_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Pipeline ETL da IBM com Astro/Airflow',
    tags=["etl", "ibm", "alphavantage"]
) as dag:

    def run_extraction():
        print(f"🔍 Running: {os.path.join(SRC_DIR, 'data_extraction.py')}")
        subprocess.run(["python", os.path.join(SRC_DIR, "data_extraction.py")], check=True)

    def run_transformation():
        print(f"🔄 Running: {os.path.join(SRC_DIR, 'data_transformation.py')}")
        subprocess.run(["python", os.path.join(SRC_DIR, "data_transformation.py")], check=True)

    def run_load():
        print(f"📥 Running: {os.path.join(SRC_DIR, 'data_load.py')}")
        subprocess.run(["python", os.path.join(SRC_DIR, "data_load.py")], check=True)

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=run_extraction
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=run_transformation
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=run_load
    )

    extract >> transform >> load
