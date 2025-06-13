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

# Ruta del ejecutable unrar
rarfile.UNRAR_TOOL = '/usr/bin/unrar'

LOCAL_PATH = '/opt/airflow/dags/archivos_descargados/'
BUCKET_NAME = 'dirisle'
REMOTE_PATH = 'data_cvs_nominal/2025'
LOGIN_URL = "http://ad.dirislimaeste.gob.pe/api/auth/login"
COPY_URL = "http://ad.dirislimaeste.gob.pe/api/auth/copyFilesNetwork"
EMAIL = "lclaudio@dirislimaeste.gob.pe"
PASSWORD = "Lr#hCfmKBE75*#W"
YEAR = 2025

os.makedirs(LOCAL_PATH, exist_ok=True)


def obtener_token():
    login_payload = {
        "email": EMAIL,
        "password": PASSWORD
    }
    response = requests.post(LOGIN_URL, json=login_payload)
    response.raise_for_status()
    return response.json()["access_token"]

def descargar_desde_sftp():
    logger = logging.getLogger(__name__)
    try:
        hook_sftp = SFTPHook(ssh_conn_id='sftp_minsa')
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
    except Exception as e:
        logger.error(f"Error en descarga desde SFTP: {e}")
        raise

def subir_rar_a_s3():
    logger = logging.getLogger(__name__)
    try:
        hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')

        if not hook_s3.check_for_bucket(BUCKET_NAME):
            raise ValueError("Bucket no disponible.")

        for f in os.listdir(LOCAL_PATH):
            if f.endswith('.rar'):
                local_file = os.path.join(LOCAL_PATH, f)
                remote_path = f"HisMinsa/2025/{f}"
                logger.info(f"Subiendo {f} a S3...")
                hook_s3.load_file(local_file, remote_path, BUCKET_NAME, replace=True)
    except Exception as e:
        logger.error(f"Error al subir archivos a S3: {e}")
        raise

def descomprimir_y_subir_csv():
    logger = logging.getLogger(__name__)
    try:
        hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')

        for f in os.listdir(LOCAL_PATH):
            if f.endswith('.rar'):
                rar_path = os.path.join(LOCAL_PATH, f)
                tmp_folder = os.path.join(LOCAL_PATH, f"tmp_{os.path.splitext(f)[0]}")
                os.makedirs(tmp_folder, exist_ok=True)

                with rarfile.RarFile(rar_path) as rf:
                    rf.extractall(tmp_folder)
                    csvs = [name for name in rf.namelist() if name.endswith('.csv')]

                for csv in csvs:
                    full_path = os.path.join(tmp_folder, csv)
                    s3_key = f"HisMinsa/2025/csv/{csv}"
                    logger.info(f"Subiendo CSV: {csv}")
                    hook_s3.load_file(full_path, s3_key, BUCKET_NAME, replace=True)
                    os.remove(full_path)

                os.remove(rar_path)
                os.rmdir(tmp_folder)
    except Exception as e:
        logger.error(f"Error al descomprimir y subir CSV: {e}")
        raise

def copia_csv_local():
    try:
        access_token = obtener_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        copy_payload = {
            "ruta_local": "/mnt/gedex_hisminsa/2025",
            "ruta_s3": "HisMinsa/2025/csv"
        }
        copy_response = requests.post(COPY_URL, json=copy_payload, headers=headers)
        copy_response.raise_for_status()

        result = copy_response.json()
        if result["error"] == 0:
            logging.info("Archivos copiados: %s", result["data"]["archivos"])
        else:
            raise Exception(f"Error al copiar archivos: {result['message']}")
    except Exception as e:
        logging.error(f"Error en copia de CSV local: {e}")
        raise

def insertar_csv_por_lotes():
    logger = logging.getLogger(__name__)
    try:
        hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')
        sql_hook = OdbcHook(odbc_conn_id='hisminsa_odbc', driver='ODBC Driver 17 for SQL Server')

        prefix = 'HisMinsa/2025/csv/'
        archivos_csv = hook_s3.list_keys(bucket_name=BUCKET_NAME, prefix=prefix)

        if not archivos_csv:
            logger.info("No se encontraron archivos CSV en S3.")
            return

        for key in archivos_csv:
            if key.endswith('.csv'):
                nombre_archivo = os.path.basename(key)  # Ej: 21_LIMA_ESTE_01.csv
                partes = nombre_archivo.replace('.csv', '').split('_')
                mes = int(partes[-1])                  # Ej: '01'

                # Ruta UNC del archivo compartido en red
                ruta_csv_red = f"\\\\192.168.10.93\\GEDEX_Descarga_HisMinsa\\2025\\{nombre_archivo}"

                # Comandos SQL
                delete_sql = f"""
                    DELETE FROM TB_HISMINSA_TEST 
                    WHERE Anio = '2025' AND Mes = {mes};
                """

                bulk_insert_sql = f"""
                    BULK INSERT TB_HISMINSA_TEST
                    FROM '{ruta_csv_red}'
                    WITH (
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\\n',
                        FIRSTROW = 2,
                        CODEPAGE = 'ACP',
                        DATAFILETYPE = 'char'
                    );
                """

                logger.info(f"Procesando archivo {nombre_archivo} para el mes {mes}")

                # Mostrar el archivo y mes mientras se hace el DELETE e INSERT
                logger.info(f"Archivo: {nombre_archivo}, Mes: {mes}")
                
                # Probar conexión ODBC antes de ejecutar los comandos
                conn = sql_hook.get_conn()
                cursor = conn.cursor()
                cursor.execute('SELECT getdate() as fecha_del_dia ;')
                result = cursor.fetchone()
                cursor.close()
                conn.close()

                if not result or not isinstance(result[0], datetime):
                    raise ValueError(f'Expected datetime, got {result}')
                logger.info(f"Conexión exitosa. Fecha actual: {result[0]}")

                # Ejecutar DELETE
                logger.info(f"Ejecutando DELETE para el archivo {nombre_archivo}...")
                sql_hook.run(delete_sql)
                logger.info(f"DELETE exitoso para {nombre_archivo}.")

                # Ejecutar BULK INSERT
                logger.info(f"Ejecutando BULK INSERT para el archivo {nombre_archivo}...")
                sql_hook.run(bulk_insert_sql)
                logger.info(f"BULK INSERT exitoso para {nombre_archivo}.")

    except Exception as e:
        logger.error(f"Error insertando CSV en SQL Server: {e}")
        raise


# DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7, 12, 0, 0),
}

with DAG(
    dag_id='dag_descarga_Hisminsa_2025',
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

    tarea_descomprimir_csv = PythonOperator(
        task_id='descomprimir_y_subir_csv',
        python_callable=descomprimir_y_subir_csv,
    )

    tarea_copia_csv_local = PythonOperator(
        task_id='copia_csv_a_server_93',
        python_callable=copia_csv_local,
    )

    tarea_insertar_en_sqlserver = PythonOperator(
        task_id='insertar_csv_en_sqlserver',
        python_callable=insertar_csv_por_lotes,
    )

    end = DummyOperator(task_id='end')

    (
        start
        >> tarea_descarga
        >> tarea_subir_rar
        >> tarea_descomprimir_csv
        >> tarea_copia_csv_local
        >> tarea_insertar_en_sqlserver
        >> end
    )
