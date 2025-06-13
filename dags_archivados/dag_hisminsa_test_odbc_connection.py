from airflow import DAG
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_hisminsa_test_odbc_connection',
    default_args=default_args,
    start_date=datetime(2025, 5, 5),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=['test', 'odbc']
) as dag:

    def test_odbc_connection():
        # Initialize the ODBC hook with the connection ID and explicit driver
        hook = OdbcHook(
            odbc_conn_id='hisminsa_odbc',
            driver='ODBC Driver 17 for SQL Server'
        )
        # Establish the connection and execute a simple query
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute('SELECT getdate() as fecha_del_dia ;')
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        # Log and validate result
        if not result or not isinstance(result[0], datetime):
            raise ValueError(f'Expected datetime, got {result}')
        print(f'Prueba de conexión exitosa Fecha actual: {result[0]}')

    test_connection = PythonOperator(
        task_id='test_connection',
        python_callable=test_odbc_connection
    )
