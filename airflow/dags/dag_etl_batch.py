from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_customers():
    input_path = '/opt/airflow/data/raw/customers.csv'
    output_path = '/opt/airflow/data/processed/customers_transformed.csv'
    df = pd.read_csv(input_path)
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df.to_csv(output_path, index=False)

with DAG(
    dag_id="etl_batch_customers",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "batch"],
) as dag:
    
    transform_task = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers
    )

    transform_task
