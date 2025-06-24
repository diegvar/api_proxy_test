from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from google.cloud import bigquery
import json
from datetime import datetime
from typing import Optional

app = FastAPI()

# Configuración
API_LOCAL_URL = "http://186.78.138.140:30000/asistencias"
PROJECT_ID = "pruebas-463316"
DATASET_ID = "Pruebas_assistencia"
TABLE_ID = "asistencias_table"

# Inicializar cliente de BigQuery
client = bigquery.Client(project=PROJECT_ID)

def create_table_if_not_exists():
    """
    Crea la tabla si no existe
    """
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Verificar si la tabla existe
        try:
            client.get_table(table_ref)
            print(f"Tabla {table_ref} ya existe")
            return True
        except Exception:
            print(f"Tabla {table_ref} no existe, creándola...")
        
        # Definir esquema de la tabla
        schema = [
        bigquery.SchemaField("identificador_rut", "STRING"),
        bigquery.SchemaField("Nombre", "STRING"),
        bigquery.SchemaField("apellido", "STRING"),
        bigquery.SchemaField("corresponde_turno", "BOOL"),
        bigquery.SchemaField("hora_entrada", "DATETIME"),
        bigquery.SchemaField("hora_salida", "DATETIME"),
        bigquery.SchemaField("Hora_marca_entrada", "DATETIME"),
        bigquery.SchemaField("Hora_marca_salida", "DATETIME"),
        bigquery.SchemaField("Marca_turno", "BOOL"),
        bigquery.SchemaField("Atraso_en_entrada", "BOOL"),
        bigquery.SchemaField("Empresa", "STRING"),
        bigquery.SchemaField("mail_empresa", "STRING"),
        bigquery.SchemaField("fecha_carga", "DATETIME"),
        bigquery.SchemaField("origen_datos", "STRING"),
    ]
        
        # Crear tabla
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        
        print(f"Tabla {table_ref} creada exitosamente")
        return True
        
    except Exception as e:
        print(f"Error al crear tabla: {str(e)}")
        return False

def replace_table_data(data):
    """
    Reemplaza todos los datos de la tabla
    """
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Eliminar todos los datos existentes
        delete_query = f"DELETE FROM `{table_ref}` WHERE 1=1"
        client.query(delete_query).result()
        print("Datos anteriores eliminados")
        
        # Insertar nuevos datos
        errors = client.insert_rows_json(table_ref, data)
        
        if errors:
            raise Exception(f"Error al insertar datos: {errors}")
        
        print(f"Datos reemplazados exitosamente: {len(data)} registros")
        return True
        
    except Exception as e:
        print(f"Error al reemplazar datos: {str(e)}")
        return False

@app.get("/")
def health_check():
    """Endpoint de salud para verificar que la API funciona"""
    return {"status": "healthy", "message": "API funcionando correctamente"}

@app.post("/sync-to-bigquery")
def sync_to_bigquery(
    empresa: Optional[str] = None,
    fecha_inicio: Optional[str] = None,
    fecha_fin: Optional[str] = None
):
    """
    Consume la API local y reemplaza los datos en BigQuery
    """
    try:
        # Preparar parámetros para la API local
        params = {}
        if empresa:
            params["empresa"] = empresa
        if fecha_inicio:
            params["fecha_inicio"] = fecha_inicio
        if fecha_fin:
            params["fecha_fin"] = fecha_fin

        # Llamar a la API local
        print(f"Llamando a API local: {API_LOCAL_URL}")
        response = requests.get(API_LOCAL_URL, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"Datos obtenidos: {len(data)} registros")

        if not data:
            return JSONResponse(content={
                "status": "success",
                "message": "No hay datos para cargar",
                "rows_inserted": 0
            })

        # Crear tabla si no existe
        if not create_table_if_not_exists():
            raise HTTPException(
                status_code=500,
                detail="Error al crear/verificar la tabla"
            )

        # Agregar timestamp de carga
        for row in data:
            row['fecha_carga'] = datetime.now().isoformat()
            row['origen_datos'] = 'api_local'

        # Reemplazar datos en BigQuery
        if not replace_table_data(data):
            raise HTTPException(
                status_code=500,
                detail="Error al reemplazar datos en BigQuery"
            )

        return JSONResponse(content={
            "status": "success",
            "message": "Datos reemplazados exitosamente en BigQuery",
            "rows_inserted": len(data),
            "table": f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        })

    except requests.RequestException as e:
        raise HTTPException(
            status_code=502, 
            detail=f"Error al conectar con la API local: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error inesperado: {str(e)}"
        )

@app.get("/data-status")
def get_data_status():
    """
    Verifica el estado de los datos en BigQuery
    """
    try:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Verificar si la tabla existe
        try:
            client.get_table(table_ref)
        except Exception:
            return JSONResponse(content={
                "table": table_ref,
                "status": "table_not_exists",
                "message": "La tabla no existe aún"
            })
        
        # Consulta para contar registros
        query = f"""
        SELECT 
            COUNT(*) as total_registros,
            MAX(fecha_carga) as ultima_carga,
            COUNT(DISTINCT empresa) as empresas_unicas
        FROM `{table_ref}`
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            return JSONResponse(content={
                "table": table_ref,
                "status": "table_exists",
                "total_registros": row.total_registros,
                "ultima_carga": row.ultima_carga,
                "empresas_unicas": row.empresas_unicas
            })
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error al consultar BigQuery: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
