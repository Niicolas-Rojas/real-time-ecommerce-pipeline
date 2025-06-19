# main.py
#!/usr/bin/env python3
"""
Script principal para inicializar el pipeline de datos de e-commerce
"""

import os
import sys
import yaml
from datetime import datetime
from google.cloud import bigquery

# Agregar el directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from data_generator import EcommerceDataGenerator
from utils import GCPClient, setup_logger

# Configurar logger
logger = setup_logger('main')

def create_bigquery_schemas():
    """Define los schemas para las tablas de BigQuery"""
    
    customers_schema = [
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("age", "INTEGER"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("registration_date", "TIMESTAMP"),
    ]
    
    products_schema = [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("subcategory", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("cost", "FLOAT"),
        bigquery.SchemaField("stock_quantity", "INTEGER"),
    ]
    
    transactions_schema = [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("quantity", "INTEGER"),
        bigquery.SchemaField("unit_price", "FLOAT"),
        bigquery.SchemaField("total_amount", "FLOAT"),
        bigquery.SchemaField("discount", "FLOAT"),
        bigquery.SchemaField("transaction_date", "TIMESTAMP"),
        bigquery.SchemaField("payment_method", "STRING"),
        bigquery.SchemaField("shipping_address", "STRING"),
    ]
    
    events_schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("page_url", "STRING"),
        bigquery.SchemaField("user_agent", "STRING"),
    ]
    
    return {
        'customers': customers_schema,
        'products': products_schema,
        'transactions': transactions_schema,
        'events': events_schema
    }

def setup_infrastructure():
    """Configura la infraestructura inicial en GCP"""
    logger.info("🚀 Configurando infraestructura en GCP...")

    # Cargar configuración
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    # Inicializar cliente GCP
    gcp_client = GCPClient()  

    # Crear dataset de BigQuery
    dataset_id = config['bigquery']['dataset_id']
    if gcp_client.create_dataset(dataset_id):           
        logger.info(f"✅ Dataset {dataset_id} configurado")
    else:
        logger.error(f"❌ Error configurando dataset {dataset_id}")
        return False

    # Crear tablas con schemas
    schemas = create_bigquery_schemas()
    table_names = config['bigquery']['tables']

    for table_key, table_name in table_names.items():
        if table_key in schemas:
            if gcp_client.create_table_from_schema(dataset_id, table_name, schemas[table_key]):   
                logger.info(f"✅ Tabla {table_name} configurada")
            else:
                logger.error(f"❌ Error configurando tabla {table_name}")
                return False
            logger.info(f"🟡 Simulando creación de tabla {table_name}")

    # Crear bucket de Cloud Storage
    bucket_name = config['storage']['bucket_name']
    if gcp_client.create_bucket(bucket_name):   
        logger.info(f"✅ Bucket {bucket_name} configurado")
    else:
        logger.error(f"❌ Error configurando bucket {bucket_name}")
        return False

    return True


def generate_sample_data():
    """Genera datos de muestra"""
    logger.info("📊 Generando datos de muestra...")
    
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    generator = EcommerceDataGenerator(config_path='config/config.yaml')
    
    # Generar cada tipo de datos
    customers = generator.generate_customers(100)
    products = generator.generate_products(50)
    transactions = generator.generate_transactions(customers, products, 200)
    events = generator.generate_user_events(customers, products, 300)

    # Guardar datos en JSON y CSV
    generator.save_data(customers, 'data/raw/customers.json', 'json')
    generator.save_data(products, 'data/raw/products.json', 'json')
    generator.save_data(transactions, 'data/raw/transactions.json', 'json')
    generator.save_data(events, 'data/raw/user_events.json', 'json')

    generator.save_data(customers, 'data/raw/customers.csv', 'csv')
    generator.save_data(products, 'data/raw/products.csv', 'csv')
    generator.save_data(transactions, 'data/raw/transactions.csv', 'csv')
    generator.save_data(events, 'data/raw/user_events.csv', 'csv')


    logger.info("✅ Datos de muestra generados exitosamente")

if __name__ == "__main__":
    logger.info("🔧 Iniciando pipeline de infraestructura y generación de datos")

    if setup_infrastructure():
        logger.info("✅ Infraestructura lista")
        generate_sample_data()
    else:
        logger.error("❌ Error al configurar la infraestructura. Abandonando ejecución.")
