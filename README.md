
# 📦 Real-Time E-commerce Pipeline

Este proyecto simula un sistema de procesamiento de datos para un sitio de e-commerce utilizando un pipeline híbrido (batch + streaming), desplegado sobre Google Cloud Platform con herramientas modernas como Apache Airflow y Apache Kafka.

---

## 🚀 Tecnologías Utilizadas

- **Google Cloud Platform (GCP)**
  - Cloud Storage: almacenamiento tipo Data Lake
  - BigQuery: almacenamiento analítico
- **Apache Airflow**: orquestación de pipelines batch
- **Apache Kafka**: procesamiento de eventos en tiempo real
- **Python 3.10**
- Librerías: `kafka-python`, `gcsfs`, `google-cloud-bigquery`
- (Opcional) Docker para contenerización

---

## 📁 Estructura del Proyecto

```
real-time-ecommerce-pipeline/
│
├── data/
│   ├── raw/                 # Datos originales (JSON, CSV)
│   └── processed/           # Datos transformados listos para análisis
│
├── kafka/
│   ├── producer/            # Produce eventos desde JSON a Kafka
│   └── consumer/            # Consume eventos y los escribe en BigQuery
│
├── dags/                    # DAGs de Airflow para procesamiento batch
├── scripts/                 # Scripts para setup de infraestructura
├── airflow/gcp-creds/       # Clave de servicio GCP (JSON)
├── main.py                  # Automatización de datasets y tablas
└── requirements.txt         # Dependencias del entorno
```

---

## 🔁 Pipeline Batch

1. Subida de archivos `raw/` a GCS con Airflow.
2. Transformación de datos y guardado en `processed/`.
3. Carga de archivos CSV transformados a BigQuery.

---

## ⚡ Pipeline Streaming

1. Un producer Kafka lee eventos `user_events.json` desde GCS.
2. Publica los eventos al topic `user-events`.
3. Un consumer Kafka los inserta en `user_events_streaming` de BigQuery:
   - Valida campos requeridos
   - Detecta duplicados antes de insertar

---

## 📊 Tablas en BigQuery

- `customers`
- `products`
- `transactions`
- `user_events` (batch)
- `user_events_streaming` (streaming)

---

## ✅ Validaciones Implementadas

- Validación de campos vacíos
- Prevención de duplicados por `event_id`
- Reporte de errores en inserción

---

## 🛠️ Cómo Ejecutar

1. **Pre-requisitos**:
   - Autenticación GCP vía JSON (`GOOGLE_APPLICATION_CREDENTIALS`)
   - Kafka corriendo en `localhost:9092`

2. **Instalación**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Infraestructura**:
   ```bash
   python main.py
   python scripts/create_streaming_dataset.py
   ```

4. **Airflow**:
   - Ejecuta manualmente los DAGs desde la UI

5. **Streaming**:
   ```bash
   python kafka/producer/kafka_producer.py
   python kafka/consumer/kafka_consumer.py
   ```

---

## 📌 Autor

Proyecto desarrollado por **Nicolás Rojas Díaz** como simulación profesional de un sistema de procesamiento de datos moderno.  
**Finalizado:** 03-07-2025
