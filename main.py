from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import requests

app = FastAPI()

# Cambia esta URL por la de tu API local o la que esté accesible desde GCP
API_LOCAL_URL = "http://186.78.138.140:30000/asistencias"

@app.get("/asistencias")
def get_asistencias(
    empresa: str = Query(None),
    fecha_inicio: str = Query(None),
    fecha_fin: str = Query(None)
):
    params = {}
    if empresa:
        params["empresa"] = empresa
    if fecha_inicio:
        params["fecha_inicio"] = fecha_inicio
    if fecha_fin:
        params["fecha_fin"] = fecha_fin

    try:
        response = requests.get(API_LOCAL_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        # Esto te dará más información en la respuesta de Cloud Run
        raise HTTPException(status_code=502, detail=f"Error al conectar con la API original: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado: {str(e)}")

    return JSONResponse(content=data)
