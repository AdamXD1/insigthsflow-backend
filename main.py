from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any, Optional
import bigquery_service # Importamos nuestro nuevo módulo
import os
from dotenv import load_dotenv
from pydantic import BaseModel # Añadido BaseModel

load_dotenv() # Carga .env en el directorio actual (backend/)

app = FastAPI(
    title="InsightsFlow API",
    version="0.1.0",
    description="Backend para la plataforma InsightsFlow, incluyendo acceso a datos de BigQuery."
)

# Habilita CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        os.getenv("FRONTEND_URL", "http://localhost:3000"),
        "https://insigthsflow-ozeryibpu-jump-ai-technologies.vercel.app",
        "https://insigthsflow.vercel.app"
    ], # Configurable por .env y producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Modelos Pydantic para las Peticiones ---
class AggregationRequest(BaseModel):
    column: str
    function: str  # Ejemplo: SUM, AVG, COUNT, MAX, MIN

class TableDataQueryRequest(BaseModel):
    table_name: str
    columns: List[str]
    limit: Optional[int] = None
    brand_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    aggregations: Optional[List[AggregationRequest]] = None
    group_by: Optional[List[str]] = None
    # Aquí se podrían añadir en el futuro: filters, group_by, aggregations, order_by

# --- Rutas de la API para BigQuery ---

@app.get("/api/v1/bigquery/tables",
         response_model=Dict[str, List[str]],
         tags=["BigQuery"],
         summary="Listar tablas de BigQuery permitidas",
         description=f"Devuelve una lista de las tablas permitidas del dataset '{bigquery_service.DATASET_ID}'.")
async def list_bigquery_tables():
    """
    Endpoint para obtener la lista de tablas de BigQuery predefinidas.
    """
    try:
        tables = bigquery_service.list_allowed_tables()
        return {"tables": tables}
    except Exception as e:
        # Aunque list_allowed_tables actualmente no lanza excepciones, es buena práctica.
        print(f"Error inesperado en list_bigquery_tables: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor al listar tablas.")


@app.get("/api/v1/bigquery/tables/{table_name}/schema",
         response_model=Dict[str, List[Dict[str, Any]]],
         tags=["BigQuery"],
         summary="Obtener esquema de una tabla de BigQuery",
         description="Devuelve el nombre y tipo de cada columna para la tabla especificada.")
async def get_bigquery_table_schema(table_name: str):
    """
    Endpoint para obtener el esquema de una tabla específica de BigQuery.
    """
    try:
        schema = bigquery_service.get_table_schema(table_name)
        return {"schema": schema}
    except ValueError as ve: # Error si la tabla no está permitida
        raise HTTPException(status_code=404, detail=str(ve))
    except FileNotFoundError as fnfe: 
        print(f"ERROR CRITICO: {str(fnfe)}")
        raise HTTPException(status_code=500, detail="Error de configuración del servidor: no se encontraron las credenciales de BigQuery.")
    except RuntimeError as re: # Error al obtener el esquema desde BQ o problema de credenciales
        # Esto puede incluir problemas de permisos o si la tabla no existe en BQ.
        raise HTTPException(status_code=500, detail=str(re))
    except Exception as e:
        # Captura general para otros posibles errores
        print(f"Error inesperado al obtener esquema para {table_name}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor al obtener esquema de tabla.")

@app.post("/api/v1/bigquery/query-data",
          response_model=Dict[str, List[Dict[str, Any]]],
          tags=["BigQuery"],
          summary="Consultar datos de una tabla de BigQuery",
          description="Ejecuta una consulta SELECT simple en una tabla permitida, con columnas y límite especificados.")
async def query_bigquery_table_data(request_body: TableDataQueryRequest):
    """
    Endpoint para obtener datos de una tabla de BigQuery.
    """
    try:
        data = bigquery_service.get_data_from_table(
            table_id=request_body.table_name,
            columns=request_body.columns,
            limit=request_body.limit,
            brand_id=request_body.brand_id,
            start_date=request_body.start_date,
            end_date=request_body.end_date,
            aggregations=request_body.aggregations,
            group_by=request_body.group_by
        )
        return {"data": data}
    except ValueError as ve: # Errores como tabla no permitida, columnas vacías, columna no existe
        raise HTTPException(status_code=400, detail=str(ve))
    except FileNotFoundError as fnfe: # Aunque menos probable aquí, por si get_bigquery_client falla
        print(f"ERROR CRITICO en query-data: {str(fnfe)}")
        raise HTTPException(status_code=500, detail="Error de configuración del servidor: credenciales de BigQuery no encontradas.")
    except RuntimeError as re: # Errores de ejecución de consulta BQ o validación de esquema
        raise HTTPException(status_code=500, detail=str(re))
    except Exception as e:
        print(f"Error inesperado al consultar datos de la tabla {request_body.table_name}: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor al consultar datos de la tabla.")


@app.get("/", include_in_schema=False) # Ocultar de la documentación de la API generada
async def read_root():
    return {"message": "Backend OK. Visita /docs para la documentación de la API."}