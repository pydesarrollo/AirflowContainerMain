import json
from datetime import datetime
import uuid
import unicodedata
import re

import pendulum
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- Constantes de Configuración ---
MINIO_CONN_ID = "minio_s3"
DB_CONN_ID = "BD93"
# Nombre del bucket es solo "temp"
MINIO_BUCKET_NAME = "temp"
# La parte dinámica de la ruta se construirá en la tarea list_json_files
MINIO_BASE_PATH = "temp/sihce" # La parte fija de la ruta

DB_TABLE = "CITAS_SIHCE_RAW_ETL"
ODBC_DRIVER = 'ODBC Driver 17 for SQL Server'

# CONSTANTES PARA CABECERAS JSON (ajusta estas si tus cabeceras son diferentes)
# Estas son las cabeceras originales que esperas en los archivos JSON
# El usuario mencionó:
# Fecha de cita, Fecha Nac
# Hora inicial cita, Hora final cita, Fecha de creación, FECHA DE MODIFICACIÓN, Fecha Reporte
# Para renaes, asumimos que se deriva de una columna como "Cod_EESS" o "Cód. EESS"
# Debes verificar la cabecera JSON exacta para el código EESS que alimenta 'renaes'.
# Ejemplo: si la cabecera es "Código de Establecimiento", pon eso.
SOURCE_HEADER_FECHA_CITA = 'Fecha de cita'
SOURCE_HEADER_FECHA_NAC = 'Fecha Nac'
SOURCE_HEADER_HORA_INICIAL_CITA = 'Hora inicial cita'
SOURCE_HEADER_HORA_FINAL_CITA = 'Hora final cita'
SOURCE_HEADER_FECHA_CREACION = 'Fecha de creación'
SOURCE_HEADER_FECHA_MODIFICACION = 'FECHA DE MODIFICACIÓN' # Tal como lo proporcionaste
SOURCE_HEADER_FECHA_REPORTE = 'Fecha Reporte'
SOURCE_HEADER_FOR_RENAES = 'Cod_EESS' # O "Cód. EESS", etc. ¡VERIFICAR ESTA CABECERA!

def normalize_column_name(col_name: str) -> str:
    """
    Normaliza un nombre de columna:
    1. Elimina espacios al inicio/final.
    2. Convierte a forma NFKD y elimina diacríticos (acentos).
    3. Convierte a minúsculas.
    4. Reemplaza secuencias de caracteres no alfanuméricos por un solo guion bajo.
    5. Reemplaza múltiples guiones bajos por uno solo.
    6. Elimina guiones bajos al inicio o al final.
    Ej: "Cód. Servicio" -> "cod_servicio"
        "FECHA DE MODIFICACIÓN" -> "fecha_de_modificacion"
    """
    if not isinstance(col_name, str):
        print(f"ADVERTENCIA: Se encontró una cabecera no string: {col_name}. Se intentará convertir a string.")
        return str(col_name) if col_name is not None else ""

    s = col_name.strip()
    s = "".join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = s.lower()
    # Reemplaza caracteres no alfanuméricos (excepto guion bajo) por guion bajo
    s = re.sub(r'[^a-z0-9_]+', '_', s)
    # Reemplaza múltiples _ con uno solo
    s = re.sub(r'_+', '_', s)
    s = s.strip('_')
    return s

def parse_date_flexible(date_str: str):
    # parse_date_flexible ya maneja cadenas vacías y None devolviendo None
    if not date_str or not isinstance(date_str, str) or not date_str.strip():
        return None
    
    formats_to_try = [
        '%d/%m/%Y %I:%M %p', # Ej: 13/06/2025 10:00 AM
        '%Y-%m-%d %H:%M:%S', # Ej: 2025-06-13 10:00:00 (aunque menos probable para estas columnas)
        '%d/%m/%Y',         # Ej: 13/06/2025
        '%Y-%m-%d',         # Ej: 2025-06-13
        '%H:%M:%S',         # Ej: 10:00:00 (para horas solas, si aplica)
        '%I:%M %p',         # Ej: 10:00 AM (para horas solas, si aplica)
        # Añadir otros formatos si son posibles
    ]
    
    date_str_stripped = date_str.strip()
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str_stripped, fmt)
        except ValueError:
            continue
            
    print(f"ADVERTENCIA: No se pudo parsear la fecha/hora '{date_str_stripped}' con los formatos conocidos.")
    return None

# --- Generar claves normalizadas una vez ---
# Estas claves se usarán para buscar en el diccionario 'record' y para definir conjuntos.
KEY_FECHA_DE_CITA_NORM = normalize_column_name(SOURCE_HEADER_FECHA_CITA)
KEY_FECHA_NAC_NORM = normalize_column_name(SOURCE_HEADER_FECHA_NAC)
KEY_HORA_INICIAL_CITA_NORM = normalize_column_name(SOURCE_HEADER_HORA_INICIAL_CITA)
KEY_HORA_FINAL_CITA_NORM = normalize_column_name(SOURCE_HEADER_HORA_FINAL_CITA)
KEY_FECHA_DE_CREACION_NORM = normalize_column_name(SOURCE_HEADER_FECHA_CREACION)
KEY_FECHA_DE_MODIFICACION_NORM = normalize_column_name(SOURCE_HEADER_FECHA_MODIFICACION)
KEY_FECHA_REPORTE_NORM = normalize_column_name(SOURCE_HEADER_FECHA_REPORTE)
KEY_COD_EESS_FOR_RENAES_NORM = normalize_column_name(SOURCE_HEADER_FOR_RENAES)

# Conjuntos para facilitar la verificación rápida
date_cols_normalized_keys = {KEY_FECHA_DE_CITA_NORM, KEY_FECHA_NAC_NORM}
datetime_cols_normalized_keys = {
    KEY_HORA_INICIAL_CITA_NORM, KEY_HORA_FINAL_CITA_NORM, KEY_FECHA_DE_CREACION_NORM,
    KEY_FECHA_DE_MODIFICACION_NORM, KEY_FECHA_REPORTE_NORM
}

# Columnas para matching y actualización
MATCH_COLS = [
    'Cod_EESS', 'Nombre_EESS', 'Nro_ticket', 'Cod_Servicio', 'Descripcion_del_servicio',
    'Profesional_de_la_Salud', 'Consultorio', 'turno', 'Fecha_de_cita', 'Hora_inicial_cita',
    'Hora_final_cita', 'Tipo_de_cupo', 'Fecha_de_creacion', 'Tipo_de_documento',
    'Numero_de_documento', 'Nombres_del_paciente', 'Apellido_paterno_del_paciente',
    'Apellido_materno_del_paciente', 'Celular', 'Departamento', 'Provincia', 'Distrito',
    'Direccion_actual', 'Genero', 'Fecha_Nac', 'Numero_de_historia', 'Archivo_clinico',
    'EESS_del_asegurado', 'Cod_EESS_del_asegurado', 'Personal_que_cita'
]

UPDATE_COLS = [
    'Estado_de_la_cita', 'renaes', 'Fecha_Reporte', 'Personal_que_modifica', 'Fecha_de_modificacion'
]

with DAG(
    dag_id="etl_minio_json_to_db_v2", # Versionado
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, 
    catchup=False,
    tags=['etl', 'minio', 'bulk-insert', 'odbc', 'normalization'],
) as dag:

    @task
    def list_json_files(**context) -> list[str]:
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        
        logical_date = context['dag_run'].logical_date
        date_path = logical_date.strftime('%Y/%m/%d')
        dynamic_prefix = f"{MINIO_BASE_PATH}/{date_path}/"
        
        print(f"Buscando archivos en el bucket '{MINIO_BUCKET_NAME}' con el prefijo dinámico '{dynamic_prefix}' (derivado de la fecha {logical_date.date()}).")

        keys = s3_hook.list_keys(bucket_name=MINIO_BUCKET_NAME, prefix=dynamic_prefix)
        
        if not keys:
             raise ValueError(f"No se encontraron objetos en el bucket '{MINIO_BUCKET_NAME}' con el prefijo '{dynamic_prefix}'.")
             
        json_files = [key for key in keys if key.endswith('.json')]
        
        if not json_files:
            raise ValueError(f"No se encontraron archivos .json en el bucket '{MINIO_BUCKET_NAME}' con el prefijo '{dynamic_prefix}'.")
            
        print(f"Archivos .json encontrados: {json_files}")
        return json_files

    @task
    def truncate_raw_table():
        print(f"VACIANDO (TRUNCATE) la tabla: {DB_TABLE}")
        db_hook = OdbcHook(odbc_conn_id=DB_CONN_ID, driver=ODBC_DRIVER)
        db_hook.run(f"TRUNCATE TABLE {DB_TABLE}")
        print("Tabla vaciada exitosamente.")

    @task
    def process_and_load_file(file_key: str):
        print(f"Iniciando procesamiento para el archivo: {file_key}")
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        db_hook = OdbcHook(odbc_conn_id=DB_CONN_ID, driver=ODBC_DRIVER)

        try:
            file_content = s3_hook.read_key(key=file_key, bucket_name=MINIO_BUCKET_NAME)
            json_data = json.loads(file_content)
        except Exception as e:
            print(f"Error al leer o parsear el archivo JSON {file_key} del bucket {MINIO_BUCKET_NAME}: {e}")
            return 

        headers_raw = json_data.get("cabecera", [])
        data_rows = json_data.get("data", [])

        if not headers_raw or not data_rows:
            print(f"Archivo {file_key} está vacío o tiene un formato 'cabecera'/'data' incorrecto. Saltando.")
            return

        json_normalized_column_keys = [normalize_column_name(h) for h in headers_raw]
        
        transformed_rows_for_insert = []
        batch_uuid_for_file = str(uuid.uuid4())

        for row_index, row_values in enumerate(data_rows):
            if len(row_values) != len(headers_raw):
                 print(f"ADVERTENCIA: La fila {row_index} en el archivo {file_key} tiene {len(row_values)} valores, pero se esperaban {len(headers_raw)} según la cabecera. Saltando fila.")
                 continue 

            record = dict(zip(json_normalized_column_keys, row_values))
            
            # --- NUEVA LÓGICA: Reemplazar cadenas vacías por None ---
            # Esto se hace antes de parsear fechas/horas, aunque parse_date_flexible
            # ya maneja '' devolviendo None, esto asegura que cualquier otra columna
            # con '' también se convierta a None.
            for key, value in record.items():
                if isinstance(value, str) and value.strip() == "": # Considerar strings con solo espacios también
                    record[key] = None
            # --- FIN NUEVA LÓGICA ---

            # Parsear fechas/horas usando las claves normalizadas
            # parse_date_flexible ya devuelve None si la entrada es None (resultado del paso anterior) o '')
            for key_norm in record.keys(): 
                val = record[key_norm] 
                if key_norm in date_cols_normalized_keys or key_norm in datetime_cols_normalized_keys:
                    record[key_norm] = parse_date_flexible(val)
            
            # Obtener el valor para 'renaes' (ya podría ser None si la columna 'Cod_EESS' estaba vacía)
            renaes_value = record.get(KEY_COD_EESS_FOR_RENAES_NORM)
            
            # Construir la tupla de valores para la inserción
            current_row_ordered_values = [record.get(key_norm) for key_norm in json_normalized_column_keys]
            
            final_tuple_values = tuple(current_row_ordered_values) + \
                                 (renaes_value, batch_uuid_for_file, file_key)
            transformed_rows_for_insert.append(final_tuple_values)
        
        if not transformed_rows_for_insert:
            print(f"No hay filas válidas transformadas para insertar del archivo {file_key}. Saltando inserción.")
            return

        final_sql_column_list = json_normalized_column_keys + ['renaes', 'BatchUUID', 'SourceFile']
        
        columns_sql = ", ".join(f"[{col}]" for col in final_sql_column_list)
        placeholders = ", ".join("?" for _ in final_sql_column_list)
        sql = f"INSERT INTO {DB_TABLE} ({columns_sql}) VALUES ({placeholders})"
        
        print(f"Preparando para insertar {len(transformed_rows_for_insert)} registros del archivo {file_key}.")

        conn = None 
        cursor = None 
        try:
            conn = db_hook.get_conn()
            cursor = conn.cursor()
            if hasattr(cursor, 'fast_executemany'):
                cursor.fast_executemany = True 
            
            cursor.executemany(sql, transformed_rows_for_insert)
            conn.commit()
            print(f"Archivo {file_key} cargado exitosamente.")
        except Exception as e:
            print(f"Ocurrió un error durante la inserción masiva para {file_key}: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    @task
    def clean_raw_table():
        """
        Elimina en CITAS_SIHCE_RAW_ETL todos los registros duplicados,
        dejando sólo uno por cada grupo definido en MATCH_COLS.
        """
        db = OdbcHook(odbc_conn_id=DB_CONN_ID, driver=ODBC_DRIVER)
        cols = ", ".join(f"[{c}]" for c in MATCH_COLS)
        # CTE con ROW_NUMBER para detectar duplicados
        dedupe_sql = f"""
        WITH CTE AS (
          SELECT *, 
            ROW_NUMBER() OVER (
              PARTITION BY {cols}
              ORDER BY BatchUUID DESC  -- o ajusta el criterio de preferencia
            ) AS rn
          FROM dbo.{DB_TABLE}
        )
        DELETE FROM CTE WHERE rn > 1;
        """
        db.run(dedupe_sql)
        print("Duplicados eliminados de raw.")


    @task
    def merge_raw_to_final():
        """
        Usa un MERGE para:
        - Actualizar en CITAS_SIHCE_TEST las filas que ya existen según MATCH_COLS,
        - Insertar las que no existen,
        basándose en la tabla staging raw CITAS_SIHCE_RAW_ETL.
        """
        db_hook = OdbcHook(odbc_conn_id=DB_CONN_ID, driver=ODBC_DRIVER)
        
        # Construimos las partes dinámicas de columna
        match_cols_sql = " AND ".join(
            f"T.[{col}] = S.[{col}]" for col in MATCH_COLS
        )
        update_set_sql = ", ".join(
            f"T.[{col}] = S.[{col}]" for col in UPDATE_COLS
        )
        all_cols = MATCH_COLS + UPDATE_COLS
        insert_cols_sql = ", ".join(f"[{col}]" for col in all_cols)
        insert_vals_sql = ", ".join(f"S.[{col}]" for col in all_cols)
        
        merge_sql = f"""
        SET NOCOUNT ON;

        -- Asegúrate de tener un índice en MATCH_COLS en CITAS_SIHCE_TEST y en raw
        MERGE INTO dbo.CITAS_SIHCE_TEST AS T
        USING dbo.CITAS_SIHCE_RAW_ETL AS S
          ON {match_cols_sql}
        WHEN MATCHED THEN
          UPDATE SET {update_set_sql}
        WHEN NOT MATCHED BY TARGET THEN
          INSERT ({insert_cols_sql})
          VALUES ({insert_vals_sql})
        ;
        """
        
        # Ejecutamos el MERGE
        db_hook.run(merge_sql)
        # Opcional: vaciar la raw si no la necesitas después
        # db_hook.run("TRUNCATE TABLE dbo.CITAS_SIHCE_RAW_ETL")
        print("Merge raw → final completado.")

    # --- Definición del Flujo de Tareas ---
    json_file_list = list_json_files() 
    
    truncate_task = truncate_raw_table()
    
    process_tasks = process_and_load_file.expand(file_key=json_file_list)
    
    #clean_task = clean_raw_table()
    
    #merge_task = merge_raw_to_final()

    # json_file_list >> truncate_task >> process_tasks >> merge_task
    #json_file_list >> truncate_task >> process_tasks >> clean_task >> merge_task
    json_file_list >> truncate_task >> process_tasks