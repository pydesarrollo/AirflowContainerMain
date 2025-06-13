from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json

# Configuración
LOGIN_URL = "http://ad.dirislimaeste.gob.pe/api/auth/login"
COPY_URL = "http://ad.dirislimaeste.gob.pe/api/auth/copyFilesNetwork"
EMAIL = "lclaudio@dirislimaeste.gob.pe"
PASSWORD = "Lr#hCfmKBE75*#W"

def login_and_copy_files():
    # Paso 1: Login
    login_payload = {
        "email": EMAIL,
        "password": PASSWORD
    }
    response = requests.post(LOGIN_URL, json=login_payload)
    response.raise_for_status()
    access_token = response.json()["access_token"]

    # Paso 2: Copia de archivos
    headers = {"Authorization": f"Bearer {access_token}"}
    copy_payload = {
        "ruta_local": "/mnt/gedex_hisminsa/2025",
        "ruta_s3": "HisMinsa/2025/csv"
    }
    copy_response = requests.post(COPY_URL, json=copy_payload, headers=headers)
    copy_response.raise_for_status()

    result = copy_response.json()
    if result["error"] == 0:
        print("Archivos copiados:", result["data"]["archivos"])
    else:
        raise Exception(f"Error al copiar archivos: {result['message']}")

# DAG Definition
with DAG(
    dag_id="dag_copia_csv_Server_93",
    start_date=days_ago(1),
    schedule_interval=None,  # Se ejecuta manualmente
    catchup=False,
    tags=["hisminsa"],
) as dag:

    ejecutar_login_y_copia = PythonOperator(
        task_id="login_y_copia_archivos",
        python_callable=login_and_copy_files
    )
