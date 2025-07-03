from google.cloud import bigquery

# Configuración del proyecto y dataset
PROJECT_ID = "real-time-ecommerce-pipeline"
DATASET_ID = "ecommerce_streaming"
LOCATION = "southamerica-east1"

def create_bigquery_dataset():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset_ref.location = LOCATION

    dataset = client.create_dataset(dataset_ref, exists_ok=True)
    print(f"✅ Dataset '{DATASET_ID}' creado o ya existe en la región.")

def create_user_events_streaming_table():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.user_events_streaming"

    schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("page_url", "STRING"),
        bigquery.SchemaField("user_agent", "STRING"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"✅ Tabla 'user_events_streaming' creada o ya existe.")

if __name__ == "__main__":
    print("🚀 Iniciando creación de dataset y tabla de streaming...")
    create_bigquery_dataset()
    create_user_events_streaming_table()
    print("🏁 Proceso completado.")
