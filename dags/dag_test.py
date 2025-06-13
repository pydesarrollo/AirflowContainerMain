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
import tempfile
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7, 12, 0, 0),
}

rarfile.UNRAR_TOOL = '/usr/bin/unrar'

LOCAL_PATH = '/opt/airflow/dags/archivos_descargados/'
REMOTE_PATH = 'data_cvs_nominal/2025'
BUCKET_NAME = 'dirisle'

def descargar_desde_sftp():
    logger = logging.getLogger(__name__)
    hook_sftp = SFTPHook(ssh_conn_id='sftp_minsa')

    # Asegurarse de que el directorio local exista
    if not os.path.exists(LOCAL_PATH):
        os.makedirs(LOCAL_PATH)
        logger.info(f"Directorio local creado en {LOCAL_PATH}")

    files = hook_sftp.list_directory(REMOTE_PATH)
    rar_files = [f for f in files if f.endswith('.rar')]

    if not rar_files:
        logger.info("No se encontraron archivos .rar.")
        return

    for f in rar_files:
        remote_file = os.path.join(REMOTE_PATH, f)
        local_file = os.path.join(LOCAL_PATH, f)
        logger.info(f"Descargando {f}")
        hook_sftp.retrieve_file(remote_file, local_file)

def subir_rar_a_s3():
    logger = logging.getLogger(__name__)
    hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')

    if not hook_s3.check_for_bucket(BUCKET_NAME):
        raise ValueError("Bucket no disponible.")

    for f in os.listdir(LOCAL_PATH):
        if f.endswith('.rar'):
            local_file = os.path.join(LOCAL_PATH, f)
            remote_path = f"HisMinsa/2025/{f}"
            logger.info(f"Subiendo {f} a S3...")
            hook_s3.load_file(local_file, remote_path, BUCKET_NAME, replace=True)

def leer_rar_s3_e_insertar_bd():
    logger = logging.getLogger(__name__)
    hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')
    db_hook = OdbcHook(odbc_conn_id='hisminsa_odbc', driver='ODBC Driver 17 for SQL Server')

    rar_keys = hook_s3.list_keys(bucket_name=BUCKET_NAME, prefix='HisMinsa/2025/')
    rar_files = [k for k in rar_keys if k.endswith('.rar')]

    for rar_key in rar_files:
        # Usamos un directorio temporal para guardar el archivo RAR descargado
        with tempfile.TemporaryDirectory() as tmp_dir:  # Usamos un directorio temporal separado para evitar el error
            rar_filename = os.path.basename(rar_key)
            rar_local_path = os.path.join(tmp_dir, rar_filename)

            # Descargamos el archivo RAR a la ruta temporal
            hook_s3.download_file(
                key=rar_key,
                bucket_name=BUCKET_NAME,
                local_path=rar_local_path  # Aquí se asegura que se guarda correctamente en el archivo
            )
            logger.info(f"Descargado {rar_key} desde S3 en {rar_local_path}")

            # Procesamos el archivo RAR
            with rarfile.RarFile(rar_local_path) as rf:
                rf.extractall(path=LOCAL_PATH)
                csvs = [f for f in rf.namelist() if f.endswith('.csv')]

            for csv_name in csvs:
                csv_path = os.path.join(LOCAL_PATH, csv_name)
                if not os.path.exists(csv_path):
                    logger.warning(f"Archivo {csv_name} no encontrado después de descompresión.")
                    continue

                try:
                    df = pd.read_csv(csv_path, sep='\t', dtype=str)
                    df = df.head(10)  # Solo tomamos las primeras 10 filas para pruebas
                    logger.info(f"Insertando {len(df)} registros desde {csv_name} a la base de datos...")

                    conn = db_hook.get_conn()
                    cursor = conn.cursor()
                    for _, row in df.iterrows():
                        values = tuple(row.fillna('').values)
                        placeholders = ','.join(['?'] * len(values))
                        query = f"INSERT INTO TB_HISMINSA_TEST VALUES ({placeholders})"
                        cursor.execute(query, values)
                    conn.commit()
                    cursor.close()
                    conn.close()
                    os.remove(csv_path)  # Elimina el archivo CSV después de procesarlo

                except Exception as e:
                    logger.error(f"Error procesando el archivo {csv_name}: {e}")
                    continue

with DAG(
    dag_id='dag_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    start = DummyOperator(task_id='start')

    tarea_descarga = PythonOperator(
        task_id='descargar_rar_desde_sftp',
        python_callable=descargar_desde_sftp,
    )

    tarea_subir_rar = PythonOperator(
        task_id='subir_rar_a_s3',
        python_callable=subir_rar_a_s3,
    )

    tarea_insertar_bd = PythonOperator(
        task_id='insertar_csv_a_bd_desde_s3',
        python_callable=leer_rar_s3_e_insertar_bd,
    )

    end = DummyOperator(task_id='end')

    start >> tarea_descarga >> tarea_subir_rar >> tarea_insertar_bd >> end
