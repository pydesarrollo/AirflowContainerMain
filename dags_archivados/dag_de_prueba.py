from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {   
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 2, 12, 0, 0),
}


def hello_world_loop():
    for palabra in ['Hola', 'Mundo']:
        print(palabra)


with DAG(
    dag_id='dag_de_prueba',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=True
    ):


    start = DummyOperator(
        task_id='start',
    )   

    prueba_python = PythonOperator( 
        task_id='prueba_python',
        python_callable=hello_world_loop,
    )

    prueba_bash = BashOperator(
        task_id='prueba_bash',
        bash_command='echo "Hola Mundo desde Bash"',
    )

    start >> prueba_python >> prueba_bash


