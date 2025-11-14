import json
import requests
import re
from datetime import datetime, timezone, timedelta
from datetime import date
from collections import defaultdict
import os
import boto3


CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
LAMBDA_INFO_UPDATE = os.environ["LAMBDA_INFO_UPDATE"]

# Fecha con la que se filtran los updates
select_fecha = date(2025, 11, 6)
# Fecha actual del sistema
#select_fecha = date.today()

# Definir el cliente de s3
s3 = boto3.client('s3')
# Definir el cliente de Lambda
lambda_client = boto3.client('lambda')


parents = []
global_id_fase = []
payload_format = """
   {
    "layerServerGens": [
        {
            "id": 0,
            "serverGen": 1801726
        }
    ],
    "transportType": "esriTransportTypeUrl",
    "responseType": "esriDataChangesResponseTypeEdits",
    "edits": [
        {
            "id": 0,
            "features": {
                "adds": [],
                "updates": [              
                ],
                "deleteIds": []
            },
            "attachments": {
                "adds": [
                    
                ],
                "updates": [],
                "deleteIds": []
            }
        },
        {
            "id": 1,
            "features": {
                "adds": [],
                "updates": [
                ],
                "deleteIds": []
            },
            
            "attachments": {
                "adds": [
                ],
                "updates": [],
                "deleteIds": []
            }
        }
    ]
}
"""

# funcion para invocar lambda que genera los archivos
def invoke_lambda(payload):

    response = lambda_client.invoke(
        FunctionName = LAMBDA_INFO_UPDATE,
        InvocationType = 'Event',  # async
        Payload = payload.encode('utf-8')  #  convierte a JSON y luego a bytes
    )
    return response 

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

def query_attachment(token,fase,payload):
    # Generamos la dirección URL que permite hacer el query de los attachments de la fase 1
        #filtar los atachments por global ids
        if fase == 0:
            globalIds = ",".join(parents)
        else:
            globalIds = ",".join(global_id_fase)

        API_query_layer = f"https://services3.arcgis.com/hrpzrRnIsS21AFPI/ArcGIS/rest/services/FINAAAAAAAL/FeatureServer/{fase}/queryAttachments"
        
        payload_API_query_layer = {
            "objectIds": "",
            "globalIds": globalIds,
            "definitionExpression": "",
            "attachmentsDefinitionExpression": "",
            "attachmentTypes": "image/jpeg,image/png,image/jpg",
            "size": "",
            "keywords": "",
            "resultOffset": "",
            "resultRecordCount": "",
            "orderByFields": "",
            "returnUrl": "true",
            "returnCountOnly": "false",
            "returnDistinctKeywords": "false",
            "cacheHint": "false",
            "f": "json",  
            "token": token
        }

        #Vamos a realizar un http get request con esta URL + el token  
        response_API_query_layer_capa_principal = requests.get(API_query_layer, params=payload_API_query_layer)
        #print("Status:", response_API_query_layer.status_code)
        #print("Respuesta:", response_API_query_layer.text[:500])  # imprime primeros 500 caracteres
        
        
        # Recibimos el JSON con la información de la capa principal
        response_API_query_layer_dict_capa_principal = response_API_query_layer_capa_principal.json()

        # Si no hay attachmentGroups o la lista está vacía, no hacer nada
        if not response_API_query_layer_dict_capa_principal.get("attachmentGroups"):
            print(f"No hay attachments en la fase {fase}.")
            return

        # Recorremos cada grupo de attachments
        for attach_group in response_API_query_layer_dict_capa_principal.get("attachmentGroups", []):
            
            parent_global_id = attach_group.get("parentGlobalId")

            # Cada grupo tiene una lista de attachments
            for attach_info in attach_group.get("attachmentInfos", []):
                
                attachmentId = attach_info.get("id")
                globalId = attach_info.get("globalId")
                contentType = attach_info.get("contentType")
                name = attach_info.get("name")
                size = attach_info.get("size")
                url = attach_info.get("url")

                # Crear bloque de actualización
                attach_update = {
                    "attachmentId": attachmentId,
                    "globalId": globalId,
                    "parentGlobalId": parent_global_id,
                    "contentType": contentType,
                    "name": name,
                    "size": size,
                    "url": url
                }

                # Agregar al diccionario principal
                payload["edits"][fase]["attachments"]["updates"].append(attach_update)

        print(f"Upload attachments fase {fase} ", payload)

def query_layer(token,fase,where_clause):
    
    # Generamos la dirección URL que permite hacer el query a la fase 1   
    API_query_layer = f"https://services3.arcgis.com/hrpzrRnIsS21AFPI/ArcGIS/rest/services/FINAAAAAAAL/FeatureServer/{fase}/query"
   
    if fase == 0:
        payload_API_query_layer = {
            "where": where_clause,
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
    print("DEBUG Status:", response_API_query_layer.status_code)
    #print("Status:", response_API_query_layer.status_code)
    #print("Respuesta:", response_API_query_layer.text[:500])  # imprime primeros 500 caracteres
    return response_API_query_layer
        
def lambda_handler(event, context):   
    
    if event:
        data_event = json.loads(event['body'])
        if "fecha" in data_event:
            select_fecha = datetime.strptime(data_event["fecha"], "%Y-%m-%d").date()
            select_circuito = None
            #print("fecha API:",select_fecha)
        elif "circuito" in data_event:
            select_circuito = data_event["circuito"]
            select_fecha = None
            #print("Circuito API:",select_circuito)
            
            
    # Cargar el formato del payload a enviar
    data = json.loads(payload_format)     
        
    # Vamos a generar un token par acceder a la API de ARCGIS y traer la fase 1
    token = http_token_request()
    
    #Request a la fase 1
    # Vamos a recibir un json con la información de la fase 1
    response_API_query_layer_fase1 = query_layer(token,1,"1=1")
    response_API_query_layer_dict_fase1 = response_API_query_layer_fase1.json()
    #print("Respuesta:", response_API_query_layer_fase1.text[:1000])
    
    
    # recorrer el json recibido (DE FASE 1) y tomar los atributos (tanto para guardar cda punto en s3 como para
    # almacenar en la base de datos)
    for feature in response_API_query_layer_dict_fase1.get("features", []):
        
        atributos = feature.get("attributes", {})
        
        # datos que necesitamos para aignar el nombre al archivo
        identificador = atributos.get("GlobalID")
        parent_id = atributos.get("PARENT_ID")
        
        
        if select_fecha:
            
            
            fecha = atributos.get("FECHA_FASE1")
            

            # Si no hay fecha, saltar este feature
            if fecha is None:
                continue

            try:
                # Convertir a entero si es posible
                fecha_edicion = int(fecha)
            except (ValueError, TypeError):
                # Si no se puede convertir, saltar
                continue
            
            fecha_timestamp = datetime.fromtimestamp(fecha_edicion / 1000)  # convertir ms a segundos
            solo_fecha = fecha_timestamp.date()  # extrae solo AAAA-MM-DD
            
            
            #filtramos por fecha para almacenar los puntos actualizados y traerlos de la capa principal   
            if solo_fecha == select_fecha:
                
                parents.append(parent_id)
                #print("Item x de parents:",parents)
                global_id_fase.append(identificador)
                
                #Guardar en el payload que se enviará a la otra lambda las actualizaciones en los
                
                # atributos de fase 1
                # crear el bloque para agregar dentro de updates
                fase1_update = {
                    "attributes": atributos,
                }
                
                # agregar al diccionario principal
                data["edits"][1]["features"]["updates"].append(fase1_update)

        elif select_circuito:
            
            circuito = atributos.get("CIRCUITO_ACU")
            
            # Si no hay circuito, saltar este feature
            if circuito is None:
                continue
            
            #filtramos por circuito para almacenar los puntos actualizados y traerlos de la capa principal   
            if circuito == select_circuito:
                
                parents.append(parent_id)
                global_id_fase.append(identificador)
                
                #Guardar en el payload que se enviará a la otra lambda las actualizaciones en los
                
                # atributos de fase 1
                # crear el bloque para agregar dentro de updates
                fase1_update = {
                    "attributes": atributos,
                }
                
                # agregar al diccionario principal
                data["edits"][1]["features"]["updates"].append(fase1_update)
            
            
    #print(data)
        
    
    #QUERY PARA CAPA PRINCIPAL
    
    #Generamos el formato para el item "where" en la peticion hhtp
    where_clause = "GlobalID IN (" + ", ".join(f"'{{{parent}}}'" for parent in parents) + ")"
    #print(where_clause)
    
    #Vamos a realizar un http get request con esta URL + el token  
    response_API_query_layer_capa_principal = query_layer(token,0,where_clause)
    #print("Status:", response_API_query_layer.status_code)
    
    #print("Respuesta:", response_API_query_layer_capa_principal.text[:1000])  # imprime primeros 500 caracteres
    
    
    # Vamos a recibir un json con la información de la capa principal
    response_API_query_layer_capa_principal_dict = response_API_query_layer_capa_principal.json()
    
    # recorrer el json recibido y tomar los atributos (tanto para guardar cda punto en s3 como para
    # almacenar en la base de datos)
    for feature in response_API_query_layer_capa_principal_dict.get("features", []):
        
        atributos = feature.get("attributes", {})
        geometria = feature.get("geometry",{})
        identificador = atributos.get("GlobalID")
        parent_id = atributos.get("PARENT_ID")
        
        
        data_update = json.loads(payload_format)
    
        # atributos de la capa_principal
        # crear el bloque para agregar dentro de updates
        capa_principal_update = {
            "attributes": atributos,
            "geometry" : geometria
        }
        
        # agregar al diccionario principal
        data["edits"][0]["features"]["updates"].append(capa_principal_update) 
        
    #print(data) 
        
    # agregamos attachments de capa principal
    query_attachment(token,0,data)
    
    # agregamos attachments de fase 1
    query_attachment(token,1,data)
    payload_changes = json.dumps(data)
    print("payload",json.dumps(data))
    
    
    #Subir a S3
    
    #Generar el nombre del archivo con un timestamp
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    
    #Carpeta principal del punto
    key = f"ArcGIS-Data/Changes/{timestamp}.json"
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=payload_changes,
        ContentType="application/json"
    )

    print(f" Subido {key}")
    
    #invoke_lambda(payload_changes)
        
    
    
    
    
                
                
                
        