from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
import requests

app = FastAPI()

# Cambia esta URL por la de tu API local o la que esté accesible desde GCP
API_LOCAL_URL = "http://TU_IP_LOCAL:8000/asistencias"

@app.get("/asistencias")
def get_asistencias(
    empresa: str = Query(None),
    fecha_inicio: str = Query(None),
    fecha_fin: str = Query(None)
):
    # Prepara los parámetros para reenviar a la API local
    params = {}
    if empresa:
        params["empresa"] = empresa
    if fecha_inicio:
        params["fecha_inicio"] = fecha_inicio
    if fecha_fin:
        params["fecha_fin"] = fecha_fin

    # Llama a la API local
    response = requests.get(API_LOCAL_URL, params=params)
    data = response.json()
    return JSONResponse(content=data)