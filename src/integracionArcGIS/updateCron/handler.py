import json
import requests
import re
import boto3
from datetime import datetime, timezone, timedelta
from datetime import date
from collections import defaultdict
import os
import urllib.parse
import time
from botocore.exceptions import NoCredentialsError
from concurrent.futures import ThreadPoolExecutor



CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]
BUCKET_NAME = os.environ["BUCKET_NAME"]



# Definir el cliente de Lambda
#lambda_client = boto3.client('lambda')
# Configurar el cliente de S3
s3 = boto3.client('s3')




# funcion para generar token de arcgis
def http_token_request():

    #1. Generar el token
    url_token = 'https://www.arcgis.com/sharing/rest/oauth2/token'
    payload_token = {
    "f": "json",
    "client_id" : CLIENT_ID,
    "client_secret" : CLIENT_SECRET,
    "grant_type" : "client_credentials",
    "expiration" : "1800"
    }

    responseToken = requests.post(url_token, data=payload_token)
    #print(responseToken)
    if responseToken.json():
        access_token = responseToken.json()['access_token']
        print(access_token)
        return access_token
    else:
        print("error en la obtencion del token") 
        
def sanitize_name(name):
    """
    Limpia el texto para usarlo en nombres de archivo o rutas S3.
    Reemplaza espacios, guiones y caracteres no válidos por guiones bajos.
    """
    if not name:
        return "sin_nombre"
    # reemplazar espacios y guiones por "_"
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", name.strip())
    
    return safe.replace(" ", "_")


def query_layer_arcgis(fase,token):
    
    # Generamos la dirección URL que permite hacer el query a la capa principal
  
    API_query_layer = f"https://services3.arcgis.com/hrpzrRnIsS21AFPI/ArcGIS/rest/services/FINAAAAAAAL/FeatureServer/{fase}/query"
    
    if fase == 0:    
        payload_API_query_layer = {
            "where": "1=1",
            "objectIds": "",
            "geometry": "",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "",
            "spatialRel": "esriSpatialRelIntersects",
            "resultType": "none",
            "distance": 0.0,
            "units": "esriSRUnit_Meter",
            "relationParam": "",
            "returnGeodetic": "false",
            "outFields": "*",
            "returnHiddenFields": "false",
            "returnGeometry": "true",
            "featureEncoding": "esriDefault",
            "multipatchOption": "xyFootprint",
            "maxAllowableOffset": "",
            "geometryPrecision": "",
            "outSR": "",
            "defaultSR": "",
            "datumTransformation": "",
            "applyVCSProjection": "false",
            "returnIdsOnly": "false",
            "returnUniqueIdsOnly": "false",
            "returnCountOnly": "false",
            "returnExtentOnly": "false",
            "returnQueryGeometry": "false",
            "returnDistinctValues": "false",
            "cacheHint": "false",
            "collation": "",
            "orderByFields": "",
            "groupByFieldsForStatistics": "",
            "outStatistics": "",
            "having": "",
            "resultOffset": "",
            "resultRecordCount": "",
            "returnZ": "false",
            "returnM": "false",
            "returnTrueCurves": "false",
            "returnExceededLimitFeatures": "true",
            "quantizationParameters": "",
            "sqlFormat": "none",
            "f": "pjson",
            "token": token
        }
    else:
        payload_API_query_layer = {
            "where": "1=1",
            "objectIds": "",
            "geometry": "",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "",
            "spatialRel": "esriSpatialRelIntersects",
            "resultType": "none",
            "distance": 0.0,
            "units": "esriSRUnit_Meter",
            "relationParam": "",
            "returnGeodetic": "false",
            "outFields": "*",
            "returnHiddenFields": "false",
            "returnGeometry": "true",
            "featureEncoding": "esriDefault",
            "multipatchOption": "xyFootprint",
            "maxAllowableOffset": "",
            "geometryPrecision": "",
            "outSR": "",
            "defaultSR": "",
            "datumTransformation": "",
            "applyVCSProjection": "false",
            "returnIdsOnly": "false",
            "returnUniqueIdsOnly": "false",
            "returnCountOnly": "false",
            "returnExtentOnly": "false",
            "returnQueryGeometry": "false",
            "returnDistinctValues": "false",
            "cacheHint": "false",
            "collation": "",
            "orderByFields": "",
            "groupByFieldsForStatistics": "",
            "outStatistics": "",
            "having": "",
            "resultOffset": "",
            "resultRecordCount": "",
            "returnZ": "false",
            "returnM": "false",
            "returnTrueCurves": "false",
            "returnExceededLimitFeatures": "true",
            "quantizationParameters": "",
            "sqlFormat": "none",
            "f": "pjson",
            "token": token
            }

    
    #Vamos a realizar un http get request con esta URL + el token  
    response_API_query_layer = requests.get(API_query_layer, params=payload_API_query_layer)
    #print("Status:", response_API_query_layer.status_code)
    #print("Respuesta:", response_API_query_layer.text[:500])  # imprime primeros 500 caracteres
    
    if fase == 0:
        s3_route = f"ArcGIS-Data/Layer-Table/capa_principal.json" 
    elif fase == 1:
        s3_route = f"ArcGIS-Data/Layer-Table/Fase1.json"
    elif fase == 2:    
        s3_route = f"ArcGIS-Data/Layer-Table/Fase2.json"
    elif fase == 3:    
        s3_route = f"ArcGIS-Data/Layer-Table/Fase3.json"
    
    # Subir el archivo a S3 (la capa principal completa)
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_route,
            Body=response_API_query_layer.content
        )
        #print(f"Archivo subido exitosamente a {bucket_name}/{s3_route}")
    except FileNotFoundError:
        print("El archivo no se encontró.")
    except NoCredentialsError:
        print("Credenciales de AWS no encontradas.")
        
            
        
def lambda_handler(event, context):

    # Vamos a generar un token par acceder a la API de ARCGIS y traer la capa principal
    token = http_token_request()
    
    # query a capa principal (todos los puntos)
    query_layer_arcgis(0,token)
    
    # query a fase 1 (todos los puntos)
    query_layer_arcgis(1,token)
    
    # query a fase 2 (todos los puntos)
    query_layer_arcgis(2,token)
    
    # query a fase 3 (todos los puntos)
    query_layer_arcgis(3,token)