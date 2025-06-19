# src/utils/upload_to_gcs.py

from google.cloud import storage
import os

def upload_folder(local_folder, bucket_name, destination_folder):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, local_folder)
            blob_path = os.path.join(destination_folder, rel_path)

            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)
            print(f"✅ Subido: {blob_path}")
