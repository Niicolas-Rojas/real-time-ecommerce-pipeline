from kafka import KafkaProducer
import json
import gcsfs

# Ruta correcta al archivo JSON en GCS
BUCKET_NAME = "ecommerce-data-lake-real-time"
FILE_PATH = "raw/user_events.json"
GCP_JSON = "airflow/gcp-creds/airflow-gcs-key.json"  # Usa '/' no '\'

# Inicializar el sistema de archivos de GCS con autenticación
fs = gcsfs.GCSFileSystem(token=GCP_JSON)

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Leer línea por línea desde GCS y enviar a Kafka
with fs.open(f"{BUCKET_NAME}/{FILE_PATH}", "r") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue  # Saltar líneas vacías

        try:
            event = json.loads(line)
            producer.send("user-events", event)
            print(f"✅ Evento enviado: {event}")
        except json.JSONDecodeError as e:
            print(f"❌ Línea inválida: {line} -> {e}")


# Asegurar que todo se haya enviado
producer.flush()
