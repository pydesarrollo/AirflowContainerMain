from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

def descargar_csv_desde_s3():
    bucket_name = 'dirisle'
    prefix = 'HisMinsa/2025/'
    carpeta_destino = '/mnt/hisminsa_local'

    hook_s3 = S3Hook(aws_conn_id='digitalocean_s3')
    archivos = hook_s3.list_keys(bucket_name=bucket_name, prefix=prefix)

    for archivo in archivos:
        if archivo.endswith('.csv'):
            nombre_archivo = os.path.basename(archivo)
            ruta_local = os.path.join(carpeta_destino, nombre_archivo)
            key_obj = hook_s3.get_key(key=archivo, bucket_name=bucket_name)
            key_obj.download_file(ruta_local)
            print(f"Descargado: {archivo} -> {ruta_local}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7),
}

with DAG(
    dag_id='dag_descargar_csv_s3_a_local',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=['hisminsa', 's3', 'csv'],
) as dag:

    tarea_descarga = PythonOperator(
        task_id='descargar_csv_s3_a_hisminsa_local',
        python_callable=descargar_csv_desde_s3
    )
