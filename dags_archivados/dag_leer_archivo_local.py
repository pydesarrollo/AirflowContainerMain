from airflow import DAG
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_hisminsa_bulk_insert_odbc',
    default_args=default_args,
    start_date=datetime(2025, 5, 7),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=True,
    tags=['hisminsa', 'bulk_insert']
) as dag:

    def ejecutar_bulk_insert():
        hook = OdbcHook(
            odbc_conn_id='hisminsa_odbc',
            driver='ODBC Driver 17 for SQL Server'  # O el 18 si tienes instalado ese
        )

        conn = hook.get_conn()
        cursor = conn.cursor()

        archivo_csv = r"\\192.168.19.80\hisminsa_local\21_LIMA_ESTE_01.csv"

        sql = f"""
        BULK INSERT TB_HISMINSA_TEST
        FROM '{archivo_csv}'
        WITH (
            FIELDTERMINATOR = ',',  
            ROWTERMINATOR = '\\n',  
            FIRSTROW = 2,           
            CODEPAGE = 'ACP',        
            DATAFILETYPE = 'char'
        );
        """

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

        print(" BULK INSERT ejecutado correctamente.")

    bulk_insert_task = PythonOperator(
        task_id='bulk_insert_csv_sqlserver',
        python_callable=ejecutar_bulk_insert
    )
