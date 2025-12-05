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
#select_fecha = date(2025, 11, 15)
#select_circuito = "ALTAVISTA CENTRO"
# Fecha actual del sistema
#select_fecha = date.today()

# Definir el cliente de s3
s3 = boto3.client('s3')
# Definir el cliente de Lambda
lambda_client = boto3.client('lambda')


#parents = []
#global_id_fase1 = []
#global_id_fase2 = []
#global_id_fase3 = []
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
        },
        {
            "id": 2,
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
            "id": 3,
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


def lambda_handler(event, context):   
    
    # 1. Cargar la estructura del payload a enviar
    data = []
    data = json.loads(payload_format)     
        
    # 2. Vamos a generar un token par acceder a la API de ARCGIS y traer la fase 1
    token = http_token_request()
    
    # 3. Si llega un evento, regitra si es el nombre de un circuito o fecha
    # (filtro para seleccionar datos que van en el payload).
    #NOTA: Si el evento viene de la API entonces sólo se actualiza la fase
    # deseada y la capa principal. Si el evento viene de Cron, entonces se actualizan
    # todas las fases y la capa principal
    
    if "body" in event: # A. Evento de la API
        
        
        data_event = json.loads(event['body'])
        
        payload_extra = {
                "forzarInforme" :data_event["forzarInforme"]
            }
        
        
        data["edits"].append(payload_extra)
        
        if "fecha" in data_event:
            select_fecha = datetime.strptime(data_event["fecha"], "%Y-%m-%d").date()
            select_circuito = None
            select_fase = int(data_event["fase"])
        
            print("fecha API:",select_fecha)
            print("fase API:",select_fase)
            
        elif "circuito" in data_event:
            select_circuito = data_event["circuito"]
            select_fecha = None
            select_fase = int(data_event["fase"])
            print("Circuito API:",select_circuito)
            print("fase API:",select_fase)
            
        # 3.A  Se recorre la fase 1 y se filtra, ya sea por fecha o circuito
        parents=[]
        parents,global_id_fase1,global_id_fase2,global_id_fase3 = filtro_layer(data,token,select_fase,"1=1",select_fecha,select_circuito,parents)  
        print("parents:", parents)
        print("global_id_fase 1:", global_id_fase1)
        print("global_id_fase 2:", global_id_fase2)
        print("global_id_fase 3:", global_id_fase3)
        # 3.A.1 agregamos attachments de la fase 
        query_attachment(parents,global_id_fase1,global_id_fase2,global_id_fase3,token,select_fase,data)  
            
            
    else: # B. Evento de Cron
        
        # Filtra por el día actual
        select_fecha = date.today()  
        #select_fecha = date(2025, 11, 18)
        select_circuito = None
        parents = []
        
        # 3.B Se recorre la fase 1 y se filtra, ya sea por fecha o circuito
        parents,global_id_fase1,global_id_fase2,global_id_fase3 = filtro_layer(data,token,1,"1=1",select_fecha,select_circuito,parents)
        # 3.B.3 agregamos attachments de fase 1
        query_attachment(parents,global_id_fase1,global_id_fase2,global_id_fase3,token,1,data)
        
        # 3.B.1 Se recorre la fase 2 y se filtra, ya sea por fecha o circuito
        parents,global_id_fase1,global_id_fase2,global_id_fase3= filtro_layer(data,token,2,"1=1",select_fecha,select_circuito,parents)
        # 3.B.4 agregamos attachments de fase 2
        query_attachment(parents,global_id_fase1,global_id_fase2,global_id_fase3,token,2,data)
        
        # 3.B.2 Se recorre la fase 3 y se filtra, ya sea por fecha o circuito
        parents,global_id_fase1,global_id_fase2,global_id_fase3 = filtro_layer(data,token,3,"1=1",select_fecha,select_circuito,parents)
        # 3.B.5 agregamos attachments de fase 3
        query_attachment(parents,global_id_fase1,global_id_fase2,global_id_fase3,token,3,data)
        
        
        
    
    
    # 3.4 QUERY PARA CAPA PRINCIPAL (donde el where se encuentra filtrado
        # por la lista de parents realizada en las fases)
    print("parents_llamado_capa_principal:",parents)
    payload_capa_principal = query_capa_principal(data,token,parents)
    #print(data) 
        
    # agregamos attachments de capa principal
    query_attachment(parents,global_id_fase1,global_id_fase2,global_id_fase3,token,0,data)

    
    payload_changes = json.dumps(data)
    print("payload FINAL",json.dumps(data))
    
    
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
    
    invoke_lambda(payload_changes)
        
    

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

def query_attachment(parents,global_id_fase1,global_id_fase2,global_id_fase3,token,fase,payload):
    
    attach_update = []
    globalIds = ""

    # Generamos la dirección URL que permite hacer el query de los attachments de la fase 1
    #filtar los atachments por global ids
    if fase == 0:
        globalIds = ",".join(parents)
    elif fase == 1:
        globalIds = ",".join(global_id_fase1)
    elif fase == 2:
        globalIds = ",".join(global_id_fase2)
    elif fase == 3:
        globalIds = ",".join(global_id_fase3)

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
    #response_API_query_layer_dict_capa_principal = response_API_query_layer_capa_principal.json()
    
    

## --- VALIDACIÓN ROBUSTA DEL JSON ---
    try:
        response_text = response_API_query_layer_capa_principal.text

        # Si viene vacío
        if not response_text.strip():
            print(" ERROR: Respuesta vacía del servidor.")
            print("Status:", response_API_query_layer_capa_principal.status_code)
            return

        # Intentar convertir a JSON
        response_API_query_layer_dict_capa_principal = response_API_query_layer_capa_principal.json()

    except Exception as e:
        print(" ERROR interpretando JSON:", e)
        print("Status:", response_API_query_layer_capa_principal.status_code)
        print("Body recibido:", response_API_query_layer_capa_principal.text[:500])
        return
# --- FIN VALIDACIÓN ---
 
    
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

def query_capa_principal(data, token, parents, max_chunk=15):
    """
    Ejecuta query de capa principal partiendo la lista parents en chunks
    para evitar errores por un where_clause demasiado grande.
    """
    
    # Si la lista es muy grande, partimos en chunks recursivamente
    if len(parents) > max_chunk:
        mid = len(parents) // 2
        first_half = parents[:mid]
        second_half = parents[mid:]
        
        # Ejecutar para ambas mitades
        query_capa_principal(data, token, first_half, max_chunk)
        query_capa_principal(data, token, second_half, max_chunk)
        return  # ya procesado
    
    # -------------------------
    #   PROCESAMIENTO NORMAL
    # -------------------------
    
    capa_principal_update = []

    # Armar el WHERE
    where_clause = "GlobalID IN (" + ", ".join(f"'{{{parent}}}'" for parent in parents) + ")"

    # Llamar API
    response_API_query_layer_capa_principal = query_layer(token, 0, where_clause)
    response_API_query_layer_capa_principal_dict = response_API_query_layer_capa_principal.json()

    # Recorrer resultados
    for feature in response_API_query_layer_capa_principal_dict.get("features", []):
        atributos = feature.get("attributes", {})
        geometria = feature.get("geometry", {})

        capa_principal_update = {
            "attributes": atributos,
            "geometry": geometria
        }

        data["edits"][0]["features"]["updates"].append(capa_principal_update)

 
def filtro_layer(data,token,fase,where_clause,select_fecha,select_circuito,parents):
    fase_update = []
    global_id_fase1 = []
    global_id_fase2 = []
    global_id_fase3 = []
    
    #Request a la fase (esta fase es la que trae el global id de los puntos
    # filtrados por fecha o por circuito) 
    
    response_API_query_layer_fase = query_layer(token,fase,where_clause)
    response_API_query_layer_dict_fase = response_API_query_layer_fase.json()
    #print("Respuesta:", response_API_query_layer_fase1.text[:1000])
    
    
    # Recorrer el json recibido  y tomar los atributos (tanto para guardar 
    # cada punto en s3 como para almacenar en la base de datos)
    for feature in response_API_query_layer_dict_fase.get("features", []):
        
        atributos = feature.get("attributes", {})
        
        # datos que necesitamos para asignar el nombre al archivo
        identificador = atributos.get("GlobalID")
        parent_id = atributos.get("PARENT_ID")
        
        
        
        if select_fecha: #Logica para filtrar por fecha
            
            if fase == 1:  
                fecha = atributos.get("FECHA_FASE1")
                
            elif fase == 2:
                fecha = atributos.get("FECHA_FASE2")
               
            elif fase == 3:
                fecha = atributos.get("FECHA_FASE3")
                
            else:
                print("No se recibió ningún parámetro válido de fase en el evento.")
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "Error en la 'fase' en parametros de funcion filtro_layer."})
                }


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
            
            
            #filtrar por fecha para almacenar los puntos actualizados y traerlos de la capa principal   
            if solo_fecha == select_fecha:
                parents.append(parent_id)
                #print("Item x de parents:",parents)
                
                if fase == 1:
                    global_id_fase1.append(identificador)
                elif fase == 2:
                    global_id_fase2.append(identificador)
                elif fase == 3:
                    global_id_fase3.append(identificador)
                else: 
                    print("ERROR, FALTA fase")
                    
                
                #Guardar en el payload que se enviará a la otra lambda las actualizaciones en los
                
                # atributos de fase 1
                # crear el bloque para agregar dentro de updates
                fase_update = {
                    "attributes": atributos,
                }
                
                # agregar al diccionario principal
                data["edits"][fase]["features"]["updates"].append(fase_update)

        elif select_circuito: #Logica para filtrar por circuito
            
            circuito = atributos.get("CIRCUITO_ACU")
            
            # Si no hay circuito, saltar este feature
            if circuito is None:
                continue
            
            #filtramos por circuito para almacenar los puntos actualizados y traerlos de la capa principal   
            if circuito == select_circuito:
           
                
                if fase == 1:
                    
                    requiere_fase1 = atributos.get("REQUIERE_FASE1")
                    if requiere_fase1 == "Si":
                        
                        global_id_fase1.append(identificador)
                        parents.append(parent_id)
                        
                        # Guardar en el payload que se enviará a la otra lambda las actualizaciones 
                        # en los atributos de fase x
                        # crear el bloque para agregar dentro de updates
                        fase_update = {
                            "attributes": atributos,
                        }
                
                        # agregar al diccionario principal
                        data["edits"][fase]["features"]["updates"].append(fase_update)
                    
                elif fase == 2:
                    requiere_fase2 = atributos.get("REQUIERE_FASE2")
                    if requiere_fase2 == "Si":
                        global_id_fase2.append(identificador)
                        parents.append(parent_id)
                        # Guardar en el payload que se enviará a la otra lambda las actualizaciones 
                        # en los atributos de fase x
                        # crear el bloque para agregar dentro de updates
                        fase_update = {
                            "attributes": atributos,
                        }
                
                        # agregar al diccionario principal
                        data["edits"][fase]["features"]["updates"].append(fase_update)
                    
                elif fase == 3:
                    requiere_fase3 = atributos.get("El_punto_requiere_fase_3")
                    if requiere_fase3 == "Si":
                        global_id_fase3.append(identificador)
                        parents.append(parent_id)
                        # Guardar en el payload que se enviará a la otra lambda las actualizaciones 
                        # en los atributos de fase x
                        # crear el bloque para agregar dentro de updates
                        fase_update = {
                            "attributes": atributos,
                        }
                
                        # agregar al diccionario principal
                        data["edits"][fase]["features"]["updates"].append(fase_update)
                    
                else: 
                    print("ERROR, FALTA fase")
                   
                
               
    return parents,global_id_fase1,global_id_fase2,global_id_fase3       
    
        
