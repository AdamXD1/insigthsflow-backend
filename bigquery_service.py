import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from typing import Optional

load_dotenv() # Carga variables de .env si existe

# --- Configuración ---
DEV_MODE = False  # Cambia a True para usar credenciales locales en desarrollo

PROJECT_ID = "services-pro-368012"  # Proyecto fijo

DATASET_ID = "pro_services_us_dwh"
ALLOWED_TABLES = [
    "fact_userinfo",
    "fact_catalogue",
    "fact_epg",
    "fact_playbackactivity",
    "fact_useractivity",
    "fact_userbillingactivity",
    "fact_registereduser",
    "fact_qoe",
    "fact_events",
    "fact_ga_events",
    "favorites_2_brandid",
    "fact_addon_session_playbackactivity",
    "fact_playbackseriesactivity",
    "fact_digital_marketing_performance"
]  # Lista explícita de tablas permitidas

#DATASET_ID = "pro_services_us_kpi"
#ALLOWED_TABLES = ["kpi_consumption_performance", "kpi_content_ranking", "kpi_content_genre_performance", "kpi_content_search", "kpi_appstore_aquisition", "kpi_channels", "kpi_viewers"]
#ALLOWED_TABLES = ["kpi_consumption_performance", "kpi_content_genre_performance", "kpi_content_search", "kpi_appstore_aquisition", "kpi_channels", "kpi_viewers"]


# --- Cliente de BigQuery ---
def get_bigquery_client():
    """Inicializa y devuelve un cliente de BigQuery usando el PROJECT_ID fijo."""
    if DEV_MODE:
        credentials_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials/bigquery-service-account.json")
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"No se encontró el archivo de credenciales en {credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    else:
        client = bigquery.Client(project=PROJECT_ID)
    return client

# --- Funciones auxiliares ---
def get_all_tables_from_dataset():
    """
    Obtiene todas las tablas disponibles en el dataset configurado.
    """
    client = get_bigquery_client()
    dataset_ref = client.dataset(DATASET_ID)
    
    try:
        tables = list(client.list_tables(dataset_ref))
        table_names = [table.table_id for table in tables]
        return table_names
    except Exception as e:
        print(f"Error al obtener las tablas del dataset '{DATASET_ID}': {e}")
        raise RuntimeError(f"No se pudieron obtener las tablas del dataset '{DATASET_ID}'. Verifica que las credenciales tienen permisos.") from e

def get_effective_allowed_tables():
    """
    Devuelve la lista efectiva de tablas permitidas.
    Si ALLOWED_TABLES contiene ["ALL"], devuelve todas las tablas del dataset.
    """
    if ALLOWED_TABLES == ["ALL"]:
        return get_all_tables_from_dataset()
    return ALLOWED_TABLES

# --- Funciones del servicio ---
def list_allowed_tables():
    """
    Devuelve la lista de tablas permitidas.
    """
    return get_effective_allowed_tables()

def get_table_schema(table_id: str):
    """
    Obtiene el esquema (columnas y sus tipos) para una tabla específica
    dentro del dataset configurado.
    """
    effective_allowed_tables = get_effective_allowed_tables()
    if table_id not in effective_allowed_tables:
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

def get_data_from_table(table_id: str, columns: list[str], limit: int = 100, brand_id: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, aggregations: Optional[list] = None, group_by: Optional[list[str]] = None, order_by: Optional[dict] = None):
    """
    Obtiene datos de una tabla específica, seleccionando columnas específicas, aplicando un límite y opcionalmente filtrando por brand_id, rango de fechas (daydate), agregaciones, group by y order by.
    - aggregations: lista de dicts {column: str, function: str}
    - group_by: lista de columnas
    - order_by: dict {'column': str, 'direction': 'ASC'|'DESC'}
    """
    if not columns and not aggregations:
        raise ValueError("Debes especificar columnas o agregaciones.")

    effective_allowed_tables = get_effective_allowed_tables()
    if table_id not in effective_allowed_tables:
        raise ValueError(f"La tabla '{table_id}' no está permitida o no existe en la lista de tablas autorizadas.")

    client = get_bigquery_client()
    table_ref_string = f"{client.project}.{DATASET_ID}.{table_id}"

    # Validar que las columnas existan en el esquema (opcional pero recomendado)
    try:
        table_schema = get_table_schema(table_id)
        schema_column_names = [field["name"] for field in table_schema]
        if columns:
            for col in columns:
                if col not in schema_column_names:
                    raise ValueError(f"La columna '{col}' no existe en la tabla '{table_id}'. Columnas disponibles: {schema_column_names}")
        if aggregations:
            for agg in aggregations:
                if agg["column"] not in schema_column_names:
                    raise ValueError(f"La columna de agregación '{agg['column']}' no existe en la tabla '{table_id}'.")
        if order_by:
            if "column" not in order_by or "direction" not in order_by:
                raise ValueError("El parámetro order_by debe tener 'column' y 'direction'.")
            if order_by["column"] not in schema_column_names:
                raise ValueError(f"La columna de ordenamiento '{order_by['column']}' no existe en la tabla '{table_id}'.")
            if order_by["direction"].upper() not in ["ASC", "DESC"]:
                raise ValueError("La dirección de ordenamiento debe ser 'ASC' o 'DESC'.")
    except Exception as e:
        raise RuntimeError(f"No se pudo validar el esquema para la tabla '{table_id}' antes de la consulta: {e}")

    select_parts = []
    
    # Manejar columnas cuando hay GROUP BY
    if group_by:
        # Solo incluir columnas que estén en GROUP BY
        for col in columns:
            if col in group_by:
                select_parts.append(col)
            else:
                # Si la columna no está en GROUP BY, verificar si está en agregaciones
                if aggregations:
                    is_aggregated = any(agg["column"] == col for agg in aggregations)
                    if not is_aggregated:
                        print(f"ADVERTENCIA: La columna '{col}' no está en GROUP BY ni es una agregación. Se omitirá del SELECT.")
                else:
                    print(f"ADVERTENCIA: La columna '{col}' no está en GROUP BY. Se omitirá del SELECT.")
    else:
        # Sin GROUP BY
        if aggregations:
            # Si hay agregaciones sin GROUP BY, solo incluir las agregaciones
            # No incluir columnas originales porque causaría error en BigQuery
            pass
        else:
            # Sin GROUP BY y sin agregaciones, incluir todas las columnas
            if columns:
                select_parts.extend(columns)
    
    # Agregar todas las agregaciones
    if aggregations:
        for agg in aggregations:
            func = agg["function"].upper()
            col = agg["column"]
            # Usar el nombre original de la columna como alias en lugar del prefijo de la función
            alias = col
            select_parts.append(f"{func}({col}) AS {alias}")
            
            # Si la columna original está en columns pero no en group_by, 
            # también agregar la columna original con el alias para compatibilidad
            if columns and col in columns and (not group_by or col not in group_by):
                # La agregación ya se agregó arriba, no necesitamos hacer nada más
                pass
    
    select_str = ", ".join(select_parts)
    query = f"SELECT {select_str} FROM `{table_ref_string}`"

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

    if group_by:
        query += " GROUP BY " + ", ".join(group_by)

    if order_by:
        query += f" ORDER BY {order_by['column']} {order_by['direction'].upper()}"

    if limit is not None:
        query += f" LIMIT {limit}"

    try:
        print("[LOG] Parámetros recibidos para construir la consulta:")
        print(f"  table_id: {table_id}")
        print(f"  columns: {columns}")
        print(f"  limit: {limit}")
        print(f"  brand_id: {brand_id}")
        print(f"  start_date: {start_date}")
        print(f"  end_date: {end_date}")
        print(f"  aggregations: {aggregations}")
        print(f"  group_by: {group_by}")
        print(f"  order_by: {order_by}")
        print(f"Ejecutando consulta en BigQuery: {query}")
        query_job = client.query(query)
        results = query_job.result()
        rows = [dict(row) for row in results]
        return rows
    except Exception as e:
        print(f"Error al ejecutar la consulta en BigQuery para la tabla {table_id}: {e}")
        raise RuntimeError(f"Error al obtener datos de la tabla '{table_id}'. Detalles: {e}")

if __name__ == '__main__':
    print(f"Intentando cargar credenciales desde: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'ruta por defecto credentials/bigquery-service-account.json')}")
    
    # Establecer la ruta base para las credenciales si no está en el entorno
    #if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
    # Esto es principalmente para pruebas locales directas de este script
     #   os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/bigquery-service-account.json"
      #  print(f"Establecida GOOGLE_APPLICATION_CREDENTIALS a: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']} para prueba local.")

    # Verifica la existencia del archivo de credenciales para depuración
    _base_dir = os.path.dirname(os.path.abspath(__file__))
    _credentials_full_path = os.path.join(_base_dir, os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    print(f"Ruta completa a credenciales que se usará: {_credentials_full_path}")
    if not os.path.exists(_credentials_full_path):
        print(f"ADVERTENCIA: El archivo de credenciales NO EXISTE en {_credentials_full_path}")
    else:
        print("Archivo de credenciales encontrado.")

    print("Tablas permitidas:", list_allowed_tables())
    
    effective_allowed_tables = get_effective_allowed_tables()
    test_table = "fact_playbackactivity" 
    if test_table in effective_allowed_tables:
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
    if effective_allowed_tables:
        test_data_table = effective_allowed_tables[0] # Usar la primera tabla permitida
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