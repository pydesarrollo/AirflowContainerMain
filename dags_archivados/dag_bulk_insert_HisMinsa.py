import os
import logging
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def descargar_y_copiar_a_red():
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    # Conexión S3 (DigitalOcean Spaces)
    hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')
    bucket_name = 'dirisle'
    s3_prefix = 'HisMinsa/2025/csv'

    # Ruta del recurso compartido en red
    network_share = r'\\192.168.10.93\GEDEX_Descarga_HisMinsa\2025'
    local_dir = '/opt/airflow/dags/archivos_descargados'

    # Crear directorio local si no existe
    os.makedirs(local_dir, exist_ok=True)

    # Listar archivos en S3
    s3_files = hook_s3.list_keys(bucket_name=bucket_name, prefix=s3_prefix)

    if not s3_files:
        logger.info("No se encontraron archivos CSV en S3.")
        return

    logger.info(f"Archivos encontrados en S3: {s3_files}")

    for s3_file in s3_files:
        if not s3_file.endswith('.csv'):
            continue

        file_name = os.path.basename(s3_file)
        local_file_path = os.path.join(local_dir, file_name)

        try:
            # Descargar archivo desde S3 a directorio local
            logger.info(f"Descargando {s3_file} desde S3...")
            tmp_file_path = hook_s3.download_file(
                key=s3_file,
                bucket_name=bucket_name,
                local_path=local_dir
            )

            # Renombrar el archivo temporal con su nombre original
            os.rename(tmp_file_path, local_file_path)

            if not os.path.exists(local_file_path):
                raise Exception(f"No se pudo descargar el archivo: {local_file_path}")

            # Mostrar contenido de depuración
            file_size = os.path.getsize(local_file_path)
            logger.info(f"Tamaño del archivo descargado {file_name}: {file_size} bytes")

            with open(local_file_path, 'rb') as f:
                preview = f.read(100)
                logger.info(f"Vista previa del archivo {file_name}: {preview}")

            # Copiar archivo al recurso compartido en red
            logger.info(f"Copiando {local_file_path} a la red compartida {network_share}...")
            shutil.copy(local_file_path, network_share)
            logger.info(f"{local_file_path} copiado correctamente a {network_share}")

            # NO eliminar archivo local (para inspección manual)
            # os.remove(local_file_path)

            # Salir tras procesar el primer archivo
            logger.info("Solo se procesó el primer archivo para depuración.")
            break

        except Exception as e:
            logger.error(f"Error al procesar {s3_file}: {e}")
            raise

# Definición del DAG
with DAG(
    dag_id='dag_descarga_y_copia_red_HisMinsa',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 5, 5),
    },
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=True
) as dag:

    procesar_archivos = PythonOperator(
        task_id='descargar_y_copiar_a_red',
        python_callable=descargar_y_copiar_a_red,
    )
