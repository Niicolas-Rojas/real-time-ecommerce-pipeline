from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# Configuración general
BUCKET_NAME = "ecommerce-data-lake-real-time"
DATASET_ID = "ecommerce_data"

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_processed_to_bigquery",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "gcs", "load"],
) as dag:

    load_customers = GCSToBigQueryOperator(
        task_id="load_customers",
        bucket=BUCKET_NAME,
        source_objects=["processed/customers_transformed.csv"],
        destination_project_dataset_table=f"{DATASET_ID}.customers",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_products = GCSToBigQueryOperator(
        task_id="load_products",
        bucket=BUCKET_NAME,
        source_objects=["processed/products_transformed.csv"],
        destination_project_dataset_table=f"{DATASET_ID}.products",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_transactions = GCSToBigQueryOperator(
        task_id="load_transactions",
        bucket=BUCKET_NAME,
        source_objects=["processed/transactions_transformed.csv"],
        destination_project_dataset_table=f"{DATASET_ID}.transactions",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    load_events = GCSToBigQueryOperator(
        task_id="load_events",
        bucket=BUCKET_NAME,
        source_objects=["processed/user_events_transformed.csv"],
        destination_project_dataset_table=f"{DATASET_ID}.user_events",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    [load_customers, load_products, load_transactions, load_events]
