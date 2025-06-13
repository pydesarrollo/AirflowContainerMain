from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

def listar_archivos_rar():
    hook = SFTPHook(ssh_conn_id='sftp_minsa')
    remote_path = 'data_cvs_nominal/2025'
    files = hook.list_directory(remote_path)
    
    archivos_rar = [f for f in files if f.endswith('.rar')]
    print("Archivos .rar encontrados en 2025:")
    for archivo in archivos_rar:
        print(archivo)

with DAG(
    dag_id='dag_listar_HisMinsa_rar',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 5, 2),
    },
    schedule_interval='@once',
    is_paused_upon_creation=True,
    catchup=False,
) as dag:

    listar_archivos = PythonOperator(
        task_id='listar_archivos_rar_2025',
        python_callable=listar_archivos_rar,
    )
