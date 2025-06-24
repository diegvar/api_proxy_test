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
PROJECT_ID = "pruebas-463316"  # Cambia por tu proyecto
DATASET_ID = "Pruebas_assistencia"  # Cambia por tu dataset
TABLE_ID = "asistencias_table"  # Cambia por tu tabla

# Inicializar cliente de BigQuery
client = bigquery.Client(project=PROJECT_ID)

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
    Consume la API local y carga los datos en BigQuery
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

        # Preparar datos para BigQuery
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Agregar timestamp de carga
        for row in data:
            row['fecha_carga'] = datetime.now().isoformat()
            row['origen_datos'] = 'api_local'

        # Cargar datos a BigQuery
        errors = client.insert_rows_json(table_ref, data)
        
        if errors:
            raise HTTPException(
                status_code=500, 
                detail=f"Error al cargar en BigQuery: {errors}"
            )

        return JSONResponse(content={
            "status": "success",
            "message": "Datos cargados exitosamente en BigQuery",
            "rows_inserted": len(data),
            "table": table_ref
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
