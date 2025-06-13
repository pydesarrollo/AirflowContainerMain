from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import logging

def listar_y_subir_a_s3():
    logger = logging.getLogger(__name__)

    # Conexión SFTP
    hook_sftp = SFTPHook(ssh_conn_id='sftp_minsa')
    remote_path = 'data_cvs_nominal/2025'
    files = hook_sftp.list_directory(remote_path)

    archivos_rar = [f for f in files if f.endswith('.rar')]
    logger.info("Archivos .rar encontrados en 2025:")
    logger.info(archivos_rar)

    if not archivos_rar:
        logger.info("No se encontraron archivos .rar en el directorio remoto.")
        return

    # Conexión S3 (DigitalOcean Spaces)
    hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')
    bucket_name = 'dirisle'

    # Verificar si el bucket existe
    if not hook_s3.check_for_bucket(bucket_name):
        raise ValueError(f"El bucket '{bucket_name}' no existe o no está accesible.")

    local_path = '/opt/airflow/dags/archivos_descargados/'

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    # Descargar y subir archivos
    for archivo in archivos_rar:
        remote_file = os.path.join(remote_path, archivo)
        local_file = os.path.join(local_path, archivo)
        try:
            logger.info(f"Descargando {archivo} desde SFTP...")
            hook_sftp.retrieve_file(remote_file, local_file)

            logger.info(f"Subiendo {archivo} a DigitalOcean Spaces...")
            remote_s3_path = f"HisMinsa/2025/{archivo}"
            hook_s3.load_file(local_file, remote_s3_path, bucket_name, replace=True)

            os.remove(local_file)
            logger.info(f"{archivo} eliminado localmente tras subirlo.")
        except Exception as e:
            logger.error(f"Error procesando {archivo}: {e}")
            raise  # ← Esto detiene el DAG con estado "failed"

# Definición del DAG
with DAG(
    dag_id='dag_descarga_HisMinsa_rar',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 5, 2),
    },
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    listar_y_subir = PythonOperator(
        task_id='listar_y_subir_archivos_rar_2025',
        python_callable=listar_y_subir_a_s3,
    )
