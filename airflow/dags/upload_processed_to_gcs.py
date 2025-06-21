from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from google.cloud import storage

def upload_processed_files_to_gcs():
    bucket_name = "ecommerce-data-lake-real-time"
    local_folder = "/opt/airflow/data/processed"
    destination_path = "processed"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for filename in os.listdir(local_folder):
        if filename.endswith(".csv"):
            local_file = os.path.join(local_folder, filename)
            blob = bucket.blob(f"{destination_path}/{filename}")
            blob.upload_from_filename(local_file)
            print(f"✅ Subido: {destination_path}/{filename}")

default_args = {
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="upload_processed_to_gcs_manual",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=["gcs", "upload", "processed"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_processed_data_to_gcs",
        python_callable=upload_processed_files_to_gcs,
    )

    upload_task
