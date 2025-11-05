import json
import boto3
import requests
import re
from datetime import datetime
import os

DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]

s3 = boto3.client('s3')

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
    

def get_attachments(data):
    
    # Configuración Arreglo
    urls = []
    
    #iteración dentro del json recibido para llegar a la url + metadata
    for edit in data.get("edits", []):
        
        layer_id = edit.get("id")
        attachments = edit.get("attachments", {})
        
        for action in ["adds", "updates"]:
            
            for att in attachments.get(action, []):
                
                url = att.get("url")
                
                if url:
                                       
                    #Tomando metadatos
                    parent_id = att.get("parentGlobalId")
                    contentType = att.get("contentType")

                    
                    #Diccionario con metadata del attachment
                    urls.append({
                        "layer_id"      : layer_id,
                        "parent_id"     : parent_id,
                        "contentType"   : contentType,
                        "url"           : url
                    })
    print(urls)               
    return urls
                                        
                                 
        

def get_feature_jsons(data):
    """
    Recorre el payload JSON de ArcGIS y genera una lista de diccionarios,
    uno por cada feature (solo adds y updates).
    """
    #donde se almacena la lista de diccionarios
    feature_jsons = []
    
    #Se recorre el payload recibido para extraer metadata
    for edit in data.get("edits", []):
        
        layer_id = edit.get("id")
        features = edit.get("features", {})

        for action in ["adds", "updates"]:
            
            for feat in features.get(action, []):
                
                attrs = feat.get("attributes", {})
                Global_ID = attrs.get("GlobalID")
                identificador = attrs.get("OBJECTID") 
                point_type = attrs.get("TIPO_PUNTO")
                geometry = feat.get("geometry")
                circuito = attrs.get("CIRCUITO_ACU")
                
                if identificador:
                    feature_jsons.append({
                        "layer_id": layer_id,
                        "OBJECTID": identificador,
                        "geometry": geometry,
                        "attributes": attrs,
                        "point_type" : point_type,
                        "GlobalID"  : Global_ID,
                        "circuito" : circuito
                        
                    })

    return feature_jsons


def lambda_handler(event, context):

    #Obtiene un diccionario tipo python de la información recibida
    data = event if isinstance(event, dict) else json.loads(event['body'])

    #1.Se extrae un diccionario con la información de los puntos added y updated realizados en arcgis
    features = get_feature_jsons(data)

    #1.1Se extrae la información del diccionario
    for feature in features:
        
        layer_id = feature["layer_id"]
        point_type = feature["point_type"]
        circuito = feature["circuito"]
        GlobalID = feature["GlobalID"]
            
        #Carpeta principal del punto
        base_prefix = f"ArcGIS-Data/Puntos/{circuito}/{GlobalID}_{point_type}/"
        
        #Generar el nombre del archivo con un timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        # Sanitizar los nombres
        safe_point_type = sanitize_name(point_type)
        safe_circuito = sanitize_name(circuito)
        filename = f"{GlobalID}__{safe_point_type}__{safe_circuito}__{timestamp}.json"

        # Determinar destino según id
        if layer_id == 0:
            key = f"{base_prefix}Capa_principal/{filename}"
        elif layer_id == 1:
            key = f"{base_prefix}Fase1/{filename}.json"
        elif layer_id == 2:
            key = f"{base_prefix}Fase2/{filename}.json"
        elif layer_id == 3:
            key = f"{base_prefix}Fase3/{filename}.json"
        else:
            continue  # ignora ids fuera de rango

        #Convertir a JSON
        json_data = json.dumps(feature, indent=2, ensure_ascii=False)

        #Subir a S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json_data,
            ContentType="application/json"
        )

        print(f" Subido {key}")
        
    
    #2. Se extrae un diccionario con la información de los attachmens added y updated realizados en arcgis
    attachments = get_attachments(data)
    #2.1 Se genera un token par acceder a la API de ARCGIS y traer los nuevos attachmnets
    token = http_token_request()
    
    #2.2 Para extraer metadata, obtener la imagen y guardar en S3
    for attach in attachments:
        
        layer_id      = attach["layer_id"]
        contentType   = attach["contentType"]
        parent_id     = attach["parent_id"]
        
        #Identificar el formato de la imagen
        contentType_parts = contentType.split('/')
        imageType = contentType_parts[1]
        url = attach["url"]
        
        
        # Paso 1: listar los circuitos dentro de "Puntos/"
        base_attach_prefix = "ArcGIS-Data/Puntos/"
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=base_attach_prefix, Delimiter='/')

        # Paso 2: buscar la subcarpeta que contiene el GlobalID
        found_prefix = None
        for prefix_info in response.get('CommonPrefixes', []):
            prefix = prefix_info['Prefix']  # ej: "ArcGIS-Data/Puntos/1402/"
            
            # listar subcarpetas dentro de ese circuito
            sub_response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/')
            
            for sub_prefix_info in sub_response.get('CommonPrefixes', []):
                sub_prefix = sub_prefix_info['Prefix']  # ej: "ArcGIS-Data/Puntos/1402/0bfc161b9eec440c8a34fdd34b22df5b_VRP/"
                pattern = rf"Puntos/.*/({GlobalID})_"
                if re.search(pattern, sub_prefix):
                    found_prefix = sub_prefix
                    break
                    if found_prefix:
                        break

                if found_prefix:
                    
                    print("Carpeta encontrada:", found_prefix)
                    
                    # Determinar ruta de destino según id
                    if layer_id == 0:
                        key = f"{found_prefix}Capa_principal/attachment__{GlobalID}__{safe_point_type}__{parent_id}.{imageType}"

                    elif layer_id == 1:
                        key = f"{found_prefix}Fase1/attachment__{GlobalID}__{safe_point_type}__{parent_id}.{imageType}"
                        
                    elif layer_id == 2:
                        key = f"{found_prefix}Fase2/attachment__{GlobalID}__{safe_point_type}__{parent_id}.{imageType}"
                        
                    elif layer_id == 3:
                        key = f"{found_prefix}Fase3/attachment__{GlobalID}__{safe_point_type}__{parent_id}.{imageType}"
                else:
                    print("No se encontró ninguna carpeta para ese GlobalID.")
                    continue  # ignora ids fuera de rango


    
            #Descargando la imagen
            response_result = requests.get(url,params={"token": token})
            
            # Ejemplo de subida
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=response_result.content,
                ContentType=contentType
            )
            print("Archivo subido correctamente a:", key)
        else:
            print(f"No se encontró ninguna carpeta que coincida con el tipo '{point_type}'.")

             

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Carga completada", "features_subidas": len(features)})
    }
