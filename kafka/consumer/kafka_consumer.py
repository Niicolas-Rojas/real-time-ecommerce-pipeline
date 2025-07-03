from kafka import KafkaConsumer
from google.cloud import bigquery
import json
import os

# Configura las credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "airflow/gcp-creds/airflow-gcs-key.json"

# Inicializar cliente de BigQuery
client = bigquery.Client()
table_id = "real-time-ecommerce-pipeline.ecommerce_streaming.user_events_streaming"

consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers="localhost:9092",
    group_id="streaming-user-events",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

required_fields = ['event_id', 'customer_id', 'event_type', 'event_timestamp']

def is_valid_event(event: dict) -> bool:
    return all(event.get(field) not in [None, ""] for field in required_fields)

def event_exists(event_id: str) -> bool:
    query = f"""
        SELECT event_id
        FROM `{table_id}`
        WHERE event_id = @event_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
        ]
    )
    result = client.query(query, job_config=job_config).result()
    return any(result)

for message in consumer:
    event = message.value

    if not is_valid_event(event):
        print(f"⚠️ Evento descartado por campos vacíos: {event}")
        continue

    event_id = event.get("event_id")
    if event_exists(event_id):
        print(f"🔁 Duplicado detectado, evento omitido: {event_id}")
        continue

    errors = client.insert_rows_json(table_id, [event])
    if errors:
        print(f"❌ Error insertando: {errors}")
    else:
        print(f"✅ Insertado: {event_id}")
