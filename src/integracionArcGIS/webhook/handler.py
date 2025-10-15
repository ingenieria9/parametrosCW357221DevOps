
import json
import urllib.parse
import re
import requests
import time
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import os


token = 0
server_gen_start = 0
server_gen_end = 0
payload = 0

# Definir el cliente de Lambda
lambda_client = boto3.client('lambda')

client_id = os.environ["ARCGIS_CLIENT_ID"]
client_secret = os.environ["ARCGIS_CLIENT_SECRET"]


# 1. Del json recibido por el evento: 
    # A.leemos body del event
    # B.Tomamos changesURL y almacenar el valor de serverGens
# 2. Vamos a generar un token par acceder a la API de ARCGIS y traer los cambios
# 3. Tenemos la dirección URL que permite detectar cuales fueron lo cambios
#    Vamos a realizar un http get request con esta URL + el token  y vamos a gregarle la variable serverGens a los parámetros de la URL
# 4. Vamos a recibir un json con la URL dónde se pueden consultar los cambios
    # A. Extraemos la URL
    # B. A la URL extraída agregamos ? + token (el ? es para agregar parámetros en la URL)
# 5. Realizamos un http get request con esta nueva URL (con token)
# 5. De la respuesta obtenida extraer result URL
# 6. Realizamos un http get request con esta URL 
# 7. Almacenar la respuesta obtenida en S3 (en formato json)
# 8. Del body del json resultante extraer:
# A. Id (# de capa) y enviar a lambda "dev-layerUpdate"
# B. attachments--> adds, updates, deletes y enviar a lambda "dev-layerUpdate"

def http_token_request():

    #1. Generar el token
    url_token = 'https://www.arcgis.com/sharing/rest/oauth2/token'
    payload_token = {
    "f": "json",
    "client_id" : client_id,
    "client_secret" : client_secret,
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
        
        
def get_urls_by_id(data):
    """
    Recorre un payload JSON de ArcGIS y devuelve un diccionario
    con el id como clave y todas las URLs (adds, updates, deletes) como lista de valores.
    Solo incluye los ids que tengan información (features o attachments).
    """
    urls_by_id = {}

    edits = data.get("edits", [])
    for edit in edits:
        edit_id = edit.get("id")
        urls = []

        attachments = edit.get("attachments", {})
        features = edit.get("features", {})

        # Recorro adds, updates y deletes de attachments
        for key in ["adds", "updates", "deletes"]:
            for att in attachments.get(key, []):
                url = att.get("url")
                if url:
                    urls.append(url)

        # Verifico si hay features con datos
        has_features = any(features.get(k, []) for k in ["adds", "updates", "deletes"])

        # Solo guardo este id si tiene urls o features
        if urls or has_features:
            urls_by_id[edit_id] = urls

    return urls_by_id



def invoke_decoder_lambda(payload):
    response = lambda_client.invoke(
        FunctionName="dev-InfoUpdate",
        InvocationType='Event',  # async
        Payload=json.dumps(payload).encode('utf-8')  #  convierte a JSON y luego a bytes
    )
    return response
 
        


def lambda_handler(event, context):
   
# 1. Del json recibido por el evento:
 
    # A.leemos body del event
    body = json.loads(event["body"])   # esto da una lista de dicts
    
    # Tomamos el primer elemento (puede haber más en lista)
    data = body[0]
    
    # B.Tomamos changesURL y almacenar el valor de serverGens
    
    #(está URL-encoded, hay que decodificarla)
    encoded_url = data["changesUrl"]
    decoded_url = urllib.parse.unquote(encoded_url)
    
    print("Decoded changesUrl:", decoded_url)
    
    # Para almacenar el valor de serverGens usamos regex para extraer los números entre [ ]
    match = re.search(r"serverGens=\[(-?\d+),(-?\d+)\]", decoded_url)
    if match:
        server_gen_start = int(match.group(1))
        server_gen_end = int(match.group(2))
        print("server_gen_start:", server_gen_start)
        print("server_gen_end:", server_gen_end)
    else:
        print("No se encontró serverGens en la URL")
        
    # 2. Vamos a generar un token par acceder a la API de ARCGIS y traer los cambios
    token = http_token_request()
    
    # 3. Generamos la dirección URL que permite detectar cuales fueron lo cambios 
    
    #first_url_API = "https://services7.arcgis.com/XPuVxG4EtJCjXhqq/ArcGIS/rest/services/MPH_EJ_0601_CIR_F00_ACU_SHP_001/FeatureServer/extractChanges"
    first_url_API = "https://services3.arcgis.com/hrpzrRnIsS21AFPI/ArcGIS/rest/services/FormularioEPM/FeatureServer/extractChanges"
    
    payload_first_url_API = {
        "serverGens": f"[{server_gen_start},{server_gen_end}]",
        "geometryType": "esriGeometryEnvelope",
        "returnInserts": "true",
        "returnUpdates": "true",
        "returnDeletes": "true",
        "returnDeletedFeatures": "true",
        "returnIdsOnly": "false",
        "returnHasGeometryUpdates": "true",
        "returnExtentOnly": "false",
        "returnAttachments": "true",
        "async": "true",
        "returnAttachmentsDataByUrl": "true",
        "transportType": "esriTransportTypeUrl",
        "dataFormat": "json",
        "changesExtentGridCell": "none",
        "f": "pjson",
        "token": token
    }

    #Vamos a realizar un http get request con esta URL + el token  y vamos a gregarle la variable serverGens a los parámetros de la URL
    response_first_url_request = requests.get(first_url_API, params=payload_first_url_API)
    # 4. Vamos a recibir un json con la URL dónde se pueden consultar los cambios y su estado
    # A. Extraemos la URL
    # La respuesta ya es JSON
    response_first_json_request = response_first_url_request.json()

    print("Status:", response_first_url_request.status_code)
    print("Respuesta:", response_first_url_request.text[:500])  # imprime primeros 500 caracteres
    
 

    second_url_API = response_first_json_request.get("statusUrl")
    print("statusUrl:", second_url_API)
    

    
    # Polling al statusUrl hasta que esté listo
    while True:
        print("Verificando estado...")
        # 5. Realizamos un http get request con esta nueva URL (con token)
        response_second_url_request   = requests.get(second_url_API, params={"f": "json","token": token})
        response_second_json_request  = response_second_url_request.json()
        print(response_second_json_request)
        estado = response_second_json_request.get("status")
        print("Estado:", estado)
        
        if estado == "Completed":
            break
        elif estado == "Failed":
            raise Exception("La tarea falló:", response_second_json_request)
        
        time.sleep(1)  # esperamos 3 seg antes de reintentar

    # 4. Extraer resultUrl
    if "resultUrl" not in response_second_json_request:
        raise Exception(f"No se encontró resultUrl en la respuesta final: {response_second_json_request}")

    result_url = response_second_json_request["resultUrl"]
    print("resultUrl:", result_url)

    # 5. Descargar JSON final con los cambios
    print("Descargando datos...")
    response_result = requests.get(result_url,params={"token": token})
    
    # 2. Generar el nombre del archivo con un timestamp
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    nombre_archivo = f"changes_{timestamp}.json"

    # 3. Configurar el cliente de S3
    s3 = boto3.client('s3')
    bucket_name = "cw357221"  
    s3_route = f"CW357221-ArcGIS-Data/Changes/{nombre_archivo}" # Opcional: define una carpeta


    # 3. Subir el archivo a S3
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_route,
            Body=response_result.content
        )
        print(f"Archivo subido exitosamente a {bucket_name}/{s3_route}")
    except FileNotFoundError:
        print("El archivo no se encontró.")
    except NoCredentialsError:
        print("Credenciales de AWS no encontradas.")
        
    # 4.Generamos el payload que será el trigger de la otra lambda
    response_json_result  = response_result.json()
    #urls_dict = get_urls_by_id(response_json_result) Mausqueherramienta para después
    payload = response_json_result
    print("Payload para generar trigger:")
    print(payload)
    print("Ejecución trigger:")
    print(invoke_decoder_lambda(payload))