from datetime import datetime
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.odbc.hooks.odbc import OdbcHook
import os
import logging
import rarfile
import pandas as pd
import requests

BUCKET_NAME = 'dirisle'

def download_json_from_s3():
    logger = logging.getLogger(__name__)
    try:
        hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')

        prefix = 'SIHCE/TEMP/'
        archivos = hook_s3.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)

        if not archivos:
            logger.info("No se encontraron archivos en S3 bajo el prefijo SIHCE/TEMP/")
            return

        logger.info("Archivos JSON encontrados:")
        for key in archivos:
            if key.endswith('.json'):
                nombre_archivo = os.path.basename(key)
                logger.info(nombre_archivo)

    except Exception as e:
        logger.error(f"Error al listar archivos JSON en S3: {e}")
        raise


# DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7, 15, 0, 0),
}

with DAG(
    dag_id='dag_download_sihce_citas',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["SIHCE"],
):

    start = DummyOperator(task_id='start')


    tarea_descargar_citas = PythonOperator(
        task_id='descargar_citas',
        python_callable=download_json_from_s3,
    )

    end = DummyOperator(task_id='end')

    (
        start
        >> tarea_descargar_citas
        >> end
    )
