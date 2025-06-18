# src/utils/gcp_client.py
import os
import yaml 
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from typing import Optional, Dict, Any
from .logger import setup_logger
from google.cloud.bigquery import WriteDisposition
import pandas as pd


# En la función:
write_disposition: str = WriteDisposition.WRITE_APPEND

logger = setup_logger(__name__)

class GCPClient:
    """Cliente unificado para servicios de Google Cloud Platform"""
    
    def __init__(self, config_path: str = "config/config.yaml", 
                 credentials_path: Optional[str] = None):
        
        # Cargar configuración
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.project_id = self.config.get('project', {}).get('gcp_project_id')
        if not self.project_id:
            raise ValueError("No se encontró 'gcp_project_id' en config.yaml")

        
        # Configurar credenciales
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.bigquery_client = bigquery.Client(
                project=self.project_id, credentials=credentials
            )
            self.storage_client = storage.Client(
                project=self.project_id, credentials=credentials
            )
        else:
            # Usar credenciales por defecto (ADC)
            self.bigquery_client = bigquery.Client(project=self.project_id)
            self.storage_client = storage.Client(project=self.project_id)
        
        logger.info(f"Cliente GCP inicializado para proyecto: {self.project_id}")
    
    def create_dataset(self, dataset_id: str) -> bool:
        """Crea un dataset en BigQuery si no existe"""
        try:
            dataset_ref = self.bigquery_client.dataset(dataset_id)
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.config['project']['region']
            
            self.bigquery_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} creado/verificado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error creando dataset {dataset_id}: {str(e)}")
            return False
    
    def create_table_from_schema(self, dataset_id: str, table_id: str, 
                                schema: list) -> bool:
        """Crea una tabla en BigQuery con el schema especificado"""
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            table = bigquery.Table(table_ref, schema=schema)
            
            self.bigquery_client.create_table(table, exists_ok=True)
            logger.info(f"Tabla {dataset_id}.{table_id} creada exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error creando tabla {table_id}: {str(e)}")
            return False
    
    def upload_json_to_bigquery(self, dataset_id: str, table_id: str, 
                               data: list, write_disposition: str = "WRITE_APPEND") -> bool:
        """Sube datos JSON a BigQuery"""
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=write_disposition,
                autodetect=True
            )
            
            job = self.bigquery_client.load_table_from_json(
                data, table_ref, job_config=job_config
            )
            job.result()  # Esperar a que termine
            
            logger.info(f"Cargados {len(data)} registros a {dataset_id}.{table_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cargando datos a BigQuery: {str(e)}")
            return False
    
    def upload_to_storage(self, bucket_name: str, source_file: str, 
                         destination_blob: str) -> bool:
        """Sube un archivo a Cloud Storage"""
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob)
            
            blob.upload_from_filename(source_file)
            logger.info(f"Archivo {source_file} subido a gs://{bucket_name}/{destination_blob}")
            return True
            
        except Exception as e:
            logger.error(f"Error subiendo archivo a Storage: {str(e)}")
            return False
    
    def create_bucket(self, bucket_name: str) -> bool:
        try:
            if self.storage_client.lookup_bucket(bucket_name):
                logger.info(f"Bucket {bucket_name} ya existe.")
                return True

            bucket = self.storage_client.bucket(bucket_name)
            bucket.location = self.config['project']['region']
            self.storage_client.create_bucket(bucket)
            logger.info(f"Bucket {bucket_name} creado exitosamente")
            return True
        except Exception as e:
            logger.error(f"Error creando bucket {bucket_name}: {str(e)}")
            return False

    
    def run_query(self, query: str) -> Optional[list]:
        """Ejecuta una consulta SQL en BigQuery"""
        try:
            query_job = self.bigquery_client.query(query)
            results = query_job.result()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {str(e)}")
            return None
    
    def get_table_info(self, dataset_id: str, table_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene información de una tabla"""
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            table = self.bigquery_client.get_table(table_ref)
            
            return {
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema': [{'name': field.name, 'type': field.field_type} 
                          for field in table.schema]
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo info de tabla: {str(e)}")
            return None
    def upload_dataframe_to_bigquery(self, dataset_id: str, table_id: str, df: pd.DataFrame,
                                  write_disposition: str = "WRITE_APPEND") -> bool:
        try:
            table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
            job = self.bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            logger.info(f"{len(df)} filas cargadas a {dataset_id}.{table_id}")
            return True
        except Exception as e:
            logger.error(f"Error cargando DataFrame a BigQuery: {str(e)}")
            return False
