import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from typing import Optional

load_dotenv() # Carga variables de .env si existe

# --- Configuración ---
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials/bigquery-service-account.json")
PROJECT_ID = None 

#DATASET_ID = "pro_services_us_dwh"
#ALLOWED_TABLES = ["favorites_2_brandid"]

DATASET_ID = "pro_services_us_kpi"
ALLOWED_TABLES = ["kpi_consumption_performance", "kpi_content_ranking", "kpi_content_genre_performance", "kpi_content_search", "kpi_appstore_aquisition", "kpi_channels", "kpi_viewers"]

#DATASET_ID = "pro_services_eu_dwh"
#ALLOWED_TABLES = ["fact_playbackactivity", "fact_registereduser", "fact_catalogue"]


# --- Cliente de BigQuery ---
def get_bigquery_client():
    """Inicializa y devuelve un cliente de BigQuery."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_full_path = os.path.join(base_dir, CREDENTIALS_PATH)

    if not os.path.exists(credentials_full_path):
        raise FileNotFoundError(f"El archivo de credenciales no se encontró en: {credentials_full_path}. Asegúrate de que 'backend/{CREDENTIALS_PATH}' existe y la variable de entorno GOOGLE_APPLICATION_CREDENTIALS está bien configurada si la usas.")

    credentials = service_account.Credentials.from_service_account_file(credentials_full_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    global PROJECT_ID
    PROJECT_ID = client.project 
    return client

# --- Funciones del servicio ---
def list_allowed_tables():
    """
    Devuelve la lista de tablas permitidas.
    """
    return ALLOWED_TABLES

def get_table_schema(table_id: str):
    """
    Obtiene el esquema (columnas y sus tipos) para una tabla específica
    dentro del dataset configurado.
    """
    if table_id not in ALLOWED_TABLES:
        raise ValueError(f"La tabla '{table_id}' no está permitida o no existe en la lista de tablas autorizadas.")

    client = get_bigquery_client()
    
    # Construir el nombre completo de la tabla para la consulta
    # El proyecto se infiere del cliente, solo necesitamos dataset y tabla.
    table_ref_string = f"{client.project}.{DATASET_ID}.{table_id}"
    
    try:
        table = client.get_table(table_ref_string) 
        schema = [{"name": field.name, "type": field.field_type} for field in table.schema]
        return schema
    except Exception as e:
        print(f"Error al obtener el esquema para {table_ref_string}: {e}")
        raise RuntimeError(f"No se pudo obtener el esquema para la tabla '{table_id}'. Verifica que la tabla existe en el dataset '{DATASET_ID}' y que las credenciales tienen permisos.") from e

def get_data_from_table(table_id: str, columns: list[str], limit: int = 100, brand_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Obtiene datos de una tabla específica, seleccionando columnas específicas, aplicando un límite y opcionalmente filtrando por brand_id y rango de fechas (daydate).
    """
    if not columns:
        raise ValueError("La lista de columnas no puede estar vacía.")

    if table_id not in ALLOWED_TABLES:
        raise ValueError(f"La tabla '{table_id}' no está permitida o no existe en la lista de tablas autorizadas.")

    client = get_bigquery_client()
    table_ref_string = f"{client.project}.{DATASET_ID}.{table_id}"

    # Validar que las columnas existan en el esquema (opcional pero recomendado)
    try:
        table_schema = get_table_schema(table_id) # Reutilizamos la función existente
        schema_column_names = [field["name"] for field in table_schema]
        for col in columns:
            if col not in schema_column_names:
                raise ValueError(f"La columna '{col}' no existe en la tabla '{table_id}'. Columnas disponibles: {schema_column_names}")
    except Exception as e:
        raise RuntimeError(f"No se pudo validar el esquema para la tabla '{table_id}' antes de la consulta: {e}")

    columns_str = ", ".join(columns)
    query = f"SELECT {columns_str} FROM `{table_ref_string}`"

    where_clauses = []
    if brand_id:
        sanitized_brand_id = brand_id.replace("'", "''")
        where_clauses.append(f"brandid = '{sanitized_brand_id}'")
    if start_date:
        where_clauses.append(f"daydate >= '{start_date}'")
    if end_date:
        where_clauses.append(f"daydate <= '{end_date}'")
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    if limit is not None:
        query += f" LIMIT {limit}"

    try:
        print(f"Ejecutando consulta en BigQuery: {query}")
        query_job = client.query(query)
        results = query_job.result()  # Espera a que la consulta termine
        rows = [dict(row) for row in results]
        return rows
    except Exception as e:
        print(f"Error al ejecutar la consulta en BigQuery para la tabla {table_id}: {e}")
        raise RuntimeError(f"Error al obtener datos de la tabla '{table_id}'. Detalles: {e}")

if __name__ == '__main__':
    print(f"Intentando cargar credenciales desde: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'ruta por defecto credentials/bigquery-service-account.json')}")
    
    # Establecer la ruta base para las credenciales si no está en el entorno
    # Esto es principalmente para pruebas locales directas de este script
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/bigquery-service-account.json"
        print(f"Establecida GOOGLE_APPLICATION_CREDENTIALS a: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']} para prueba local.")


    # Verifica la existencia del archivo de credenciales para depuración
    _base_dir = os.path.dirname(os.path.abspath(__file__))
    _credentials_full_path = os.path.join(_base_dir, os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    print(f"Ruta completa a credenciales que se usará: {_credentials_full_path}")
    if not os.path.exists(_credentials_full_path):
        print(f"ADVERTENCIA: El archivo de credenciales NO EXISTE en {_credentials_full_path}")
    else:
        print("Archivo de credenciales encontrado.")

    print("Tablas permitidas:", list_allowed_tables())
    
    test_table = "fact_playbackactivity" 
    if test_table in ALLOWED_TABLES:
        try:
            print(f"Obteniendo esquema para '{test_table}'...")
            schema = get_table_schema(test_table)
            print(f"Esquema para '{test_table}':")
            for col in schema:
                print(f"  - {col['name']}: {col['type']}")
        except Exception as e:
            print(f"Error en la prueba de obtención de esquema: {e}")
            if isinstance(e, FileNotFoundError):
                print("Asegúrate que el archivo 'credentials/bigquery-service-account.json' existe en la carpeta 'backend/credentials/'.")
            elif "Could not automatically determine project ID" in str(e) or "invalid_grant" in str(e):
                 print("Error de autenticación con Google Cloud. Verifica que el archivo service account es válido y tiene permisos para BigQuery.")

    else:
        print(f"La tabla de prueba '{test_table}' no está en la lista de tablas permitidas para la prueba.") 

    # --- Prueba para get_data_from_table ---
    if ALLOWED_TABLES:
        test_data_table = ALLOWED_TABLES[0] # Usar la primera tabla permitida
        try:
            print(f"\nIntentando obtener datos de la tabla '{test_data_table}'...")
            # Intentar obtener las primeras 2 columnas del esquema para la prueba
            sample_schema = get_table_schema(test_data_table)
            if len(sample_schema) >= 2:
                sample_columns = [sample_schema[0]['name'], sample_schema[1]['name']]
            elif len(sample_schema) == 1:
                sample_columns = [sample_schema[0]['name']]
            else:
                print(f"La tabla '{test_data_table}' no tiene suficientes columnas para la prueba de datos.")
                sample_columns = []

            if sample_columns:
                # Para la prueba, podríamos o no tener un brand_id. Si lo tuviéramos, lo añadiríamos aquí.
                # Por ahora, probamos sin brand_id específico en el test, o con uno de ejemplo si fuera necesario.
                data = get_data_from_table(test_data_table, sample_columns, limit=5)
                print(f"Datos obtenidos de '{test_data_table}' (primeras 5 filas, columnas: {', '.join(sample_columns)}):")
                for row_idx, row_data in enumerate(data):
                    print(f"  Fila {row_idx + 1}: {row_data}")
                if not data:
                    print("  No se devolvieron datos o la tabla está vacía.")
            
        except Exception as e:
            print(f"Error en la prueba de obtención de datos: {e}") 