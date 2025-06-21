from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

BUCKET = "ecommerce-data-lake-real-time"
PROCESSED_PATH = '/opt/airflow/data/processed'

def transform_customers():
    df = pd.read_csv(f'gs://{BUCKET}/raw/customers.csv')
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    df.to_csv(f'{PROCESSED_PATH}/customers_transformed.csv', index=False)

def transform_products():
    df = pd.read_csv(f'gs://{BUCKET}/raw/products.csv')
    df['profit_margin'] = df['price'] - df['cost']
    df.to_csv(f'{PROCESSED_PATH}/products_transformed.csv', index=False)

def transform_transactions():
    df = pd.read_csv(f'gs://{BUCKET}/raw/transactions.csv')
    df['total_paid'] = df['quantity'] * df['unit_price'] - df['discount']
    df.to_csv(f'{PROCESSED_PATH}/transactions_transformed.csv', index=False)

def transform_user_events():
    df = pd.read_csv(f'gs://{BUCKET}/raw/user_events.csv')
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    df['event_hour'] = df['event_timestamp'].dt.hour
    df.to_csv(f'{PROCESSED_PATH}/user_events_transformed.csv', index=False)

with DAG(
    dag_id="etl_batch_all_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "batch", "initial"],
) as dag:

    transform_customers_task = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers
    )

    transform_products_task = PythonOperator(
        task_id="transform_products",
        python_callable=transform_products
    )

    transform_transactions_task = PythonOperator(
        task_id="transform_transactions",
        python_callable=transform_transactions
    )

    transform_events_task = PythonOperator(
        task_id="transform_user_events",
        python_callable=transform_user_events
    )

    # Definir orden de ejecución (en paralelo)
    [transform_customers_task, transform_products_task, transform_transactions_task, transform_events_task]
