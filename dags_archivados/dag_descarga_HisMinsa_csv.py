import os
import logging
import rarfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

# Establecer la ruta del ejecutable unrar manualmente
rarfile.UNRAR_TOOL = '/usr/bin/unrar'

def descomprimir_y_subir_csv_a_s3():
    logger = logging.getLogger(__name__)

    # Conexión SFTP
    hook_sftp = SFTPHook(ssh_conn_id='sftp_minsa')
    remote_path = 'data_cvs_nominal/2025'
    files = hook_sftp.list_directory(remote_path)

    archivos_rar = [f for f in files if f.endswith('.rar')]
    logger.info("Archivos .rar encontrados para procesar:")
    logger.info(archivos_rar)

    if not archivos_rar:
        logger.info("No se encontraron archivos .rar en el directorio remoto.")
        return

    # Conexión S3 (DigitalOcean Spaces)
    hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')
    bucket_name = 'dirisle'

    if not hook_s3.check_for_bucket(bucket_name):
        raise ValueError(f"El bucket '{bucket_name}' no existe o no está accesible.")

    local_path = '/opt/airflow/dags/archivos_descargados/'

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    # Descargar, descomprimir y subir archivos CSV
    for archivo in archivos_rar:
        remote_file = os.path.join(remote_path, archivo)
        local_file = os.path.join(local_path, archivo)
        try:
            logger.info(f"Descargando {archivo} desde SFTP...")
            hook_sftp.retrieve_file(remote_file, local_file)

            with rarfile.RarFile(local_file) as rf:
                rf.extractall(local_path)
                extracted_files = rf.namelist()

            csv_files = [f for f in extracted_files if f.endswith('.csv')]
            logger.info("Archivos .csv extraídos:")
            logger.info(csv_files)

            for csv_file in csv_files:
                full_path = os.path.join(local_path, csv_file)
                remote_s3_path = f"HisMinsa/2025/csv/{csv_file}"
                logger.info(f"Subiendo {csv_file} a S3...")
                hook_s3.load_file(full_path, remote_s3_path, bucket_name, replace=True)
                os.remove(full_path)
                logger.info(f"{csv_file} eliminado localmente tras subirlo.")

            os.remove(local_file)
            logger.info(f"{archivo} eliminado localmente.")

        except Exception as e:
            logger.error(f"Error procesando {archivo}: {e}")
            raise

# Definición del DAG
with DAG(
    dag_id='dag_descarga_HisMinsa_csv',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 5, 5),
    },
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    procesar_archivos = PythonOperator(
        task_id='descomprimir_y_subir_csv_2025',
        python_callable=descomprimir_y_subir_csv_a_s3,
    )