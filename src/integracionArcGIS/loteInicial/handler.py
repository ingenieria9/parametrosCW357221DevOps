from urllib import response
import boto3
import json
import os
import json
import urllib.parse
import re
import requests
import time
from botocore.exceptions import NoCredentialsError
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone


s3 = boto3.client('s3')
client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]




def formatear_timestamp_para_sql(fecha_dt):
    """Convierte distintos tipos de fecha → str SQL compatible con timestamptz"""
    if not fecha_dt:
        return "NULL"

    try:
        # Si viene como número (epoch en segundos o milisegundos)
        if isinstance(fecha_dt, (int, float)):
            # Si el número es muy grande (> 10^11), probablemente está en milisegundos
            if fecha_dt > 1e11:
                fecha_dt = datetime.fromtimestamp(fecha_dt / 1000, tz=timezone.utc)
            else:
                fecha_dt = datetime.fromtimestamp(fecha_dt, tz=timezone.utc)

        # Si viene como string ISO o formato común
        elif isinstance(fecha_dt, str):
            try:
                fecha_dt = datetime.fromisoformat(fecha_dt)
            except ValueError:
                try:
                    fecha_dt = datetime.strptime(fecha_dt, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print(f"No se reconoce formato de fecha: {fecha_dt}")
                    return "NULL"

            if fecha_dt.tzinfo is None:
                fecha_dt = fecha_dt.replace(tzinfo=timezone.utc)

        # Si ya es datetime sin tzinfo
        elif isinstance(fecha_dt, datetime):
            if fecha_dt.tzinfo is None:
                fecha_dt = fecha_dt.replace(tzinfo=timezone.utc)

        else:
            print(f"Tipo de fecha no reconocido: {type(fecha_dt)}")
            return "NULL"

        # Formato compatible con PostgreSQL timestamptz
        return fecha_dt.strftime("'%Y-%m-%d %H:%M:%S%z'")

    except Exception as e:
        print(f"Error al formatear fecha {fecha_dt}: {e}")
        return "NULL"
    
    


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


def save_feature_to_tmp(identificador, point_type, atributos, circuito, geometria):
    """Guarda el JSON de un punto en /tmp y devuelve la ruta."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    # Sanitizar los nombres
    safe_point_type = sanitize_name(point_type)
    safe_circuito = sanitize_name(circuito)

    filename = f"{identificador}__{safe_point_type}__{safe_circuito}__{timestamp}.json"
    filepath = os.path.join("/tmp", filename)
    
    json_data = {**atributos,**geometria}

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)

    return filepath


def upload_batch_to_s3(filepaths, max_threads=5):
    """Sube un grupo de archivos a S3 en paralelo."""
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(upload_file_to_s3, filepaths)


def upload_file_to_s3(filepath):
    """Sube un archivo a S3."""
    try:
        filename = os.path.basename(filepath)
        # Extraer los campos según el formato con "__"
        parts = filename.replace(".json", "").split("__")
        if len(parts) < 4:
            print(f" Nombre inesperado: {filename}")
            return

        identificador, point_type, circuito, timestamp = parts[:4]

        base_prefix = f"ArcGIS-Data/Puntos/{circuito}/{identificador}_{point_type}/"
        key = f"{base_prefix}Capa_principal/{filename}"

        s3.upload_file(filepath, BUCKET_NAME, key)
        print(f" Subido: s3://{BUCKET_NAME}/{key}")

    except Exception as e:
        print(f" Error subiendo {filepath}: {e}")

        
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
        
        
def invoke_lambda_db(payload, FunctionName):
    response = client_lambda_db.invoke(
        FunctionName=FunctionName,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )
    
    # Lee el cuerpo de la respuesta
    result = response["Payload"].read().decode("utf-8")
    
    # Intenta parsear a JSON si es posible
    try:
        print(json.loads(result))
        return json.loads(result)
    except json.JSONDecodeError:
        return {"raw_response": result}

def lambda_handler(event, context):

    
    puntos_capa_principal_fields = ["GlobalID","TIPO_PUNTO","FECHA_CREACION","FECHA_EDICION","CIRCUITO_ACU","SUBCIRCUIT_ACU",
                                "CUENCA_ALC","FID_ELEM","DIRECCION_ACU","CODIGO_CAJA_ACU","x","y","PUNTO_EXISTENTE","IPID_ELEM_ACU",
                                "IPID_ALC","FASE_INICIAL","VARIABLE_A_MEDIR"]
    
    # Vamos a generar un token par acceder a la API de ARCGIS y traer la capa principal
    token = http_token_request()
    
    # Generamos la dirección URL que permite hacer el query a la capa principal
  
    API_query_layer = "https://services3.arcgis.com/hrpzrRnIsS21AFPI/ArcGIS/rest/services/FINAAAAAAAL/FeatureServer/0/query"
    
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
    
    
    s3_route = f"ArcGIS-Data/Layer-Table/capa_principal.json" 
    
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
        
    # Se va a recorrer todos los puntos de la capa y cada punto se almacena en un archivo
    
    # Vamos a recibir un json con la información de la capa principal
    response_API_query_layer_dict = response_API_query_layer.json()
    
    # parametros para almacenamiento por lotes en s3 (primero almacenando en temp y luego cargando en s3 por hilos)
    batch_size = 50  # número de puntos por lote
    tmp_files = []
    
    # parametros para almacenamiento en base de datos
    insert_values = []
    
    # recorrer el json recibido y tomar los atributos (tanto para guardar cda punto en s3 como para
    # almacenar en la base de datos)
    for feature in response_API_query_layer_dict.get("features", []):
        atributos = feature.get("attributes", {})
        geometria = feature.get("geometry",{})
        coordenada_x = geometria.get("x")
        coordenada_y = geometria.get("y")
        print("x: " , coordenada_x)
        print("y: " , coordenada_y)
 

        
        # datos que necesitamos para aignar el nombre al archivo
        identificador = atributos.get("GlobalID")
        point_type = atributos.get("TIPO_PUNTO")
        valor = atributos.get("CIRCUITO_ACU")
        if valor is not None:
            circuito = valor.replace(" ", "_")
        else:
            circuito = "CAMARA"  


        
        # Construir fila SQL solo los campos de la lista (para almacenar en la base de datos)
        values = []
        for field in puntos_capa_principal_fields:
            

            val = atributos.get(field)
            
            if field.lower() == "x":
                val = coordenada_x
            elif field.lower() == "y":
                val = coordenada_y
            if val is None:
                values.append("NULL")
            elif "FECHA" in field.upper():
                # Si el campo es una fecha → formatear como timestamptz
                values.append(formatear_timestamp_para_sql(val))                
            elif isinstance(val, str):
                val = val.replace("'", "''")  # escapar comillas simples
                values.append(f"'{val}'")
            else:
                values.append(str(val))
        insert_values.append(f"({', '.join(values)})")

        # Guardar en /tmp (local) (para almacenar luego en s3)
        path = save_feature_to_tmp(identificador, point_type, atributos, circuito, geometria)
        tmp_files.append(path)

        # Si ya tenemos un lote completo, subimos a s3 y enviamos la información a la lambda que conecta 
        # con la base de datos
        if ( (len(tmp_files) >= batch_size)  ):
            
            # --- Construir el INSERT (para la base de datos)---
            insert_sql = f"""
                INSERT INTO puntos_capa_principal ({', '.join(f'"{f}"' for f in puntos_capa_principal_fields)})
                VALUES {', '.join(insert_values)}
                ON CONFLICT ("GlobalID") DO NOTHING;
            """
            
            print("insert_sql", insert_sql)
            print("values", values)
            
            # --- Crear payload para Lambda a la que se envía información para la base de datos ---
            payload_db = {
                "queryStringParameters": {
                    "query": insert_sql,
                    "db_name": "parametros",
                    "time_column": "FECHA_CREACION",
                }
            }
            
            print(payload_db)
            
            # Invocar la lambda con el lote
            invoke_lambda_db(payload_db, DB_ACCESS_LAMBDA_ARN)

            
            # para guardar en s3
            upload_batch_to_s3(tmp_files)
            
            # Borrar los archivos locales
            for f in tmp_files:
                os.remove(f)
            tmp_files = []
            
            
    # Subir el último lote (si quedó incompleto)
    if tmp_files:
        upload_batch_to_s3(tmp_files)
        for f in tmp_files:
            os.remove(f)
            
        # --- Construir el INSERT (para la base de datos)---
            insert_sql = f"""
                INSERT INTO puntos_capa_principal ({', '.join(f'"{f}"' for f in puntos_capa_principal_fields)})
                VALUES {', '.join(insert_values)}
                ON CONFLICT ("GlobalID") DO NOTHING;
            """
            
            print("insert_sql", insert_sql)
            print("values", values)
            
            # --- Crear payload para Lambda a la que se envía información para la base de datos ---
            payload_db = {
                "queryStringParameters": {
                    "query": insert_sql,
                    "db_name": "parametros",
                    "time_column": "FECHA_CREACION",
                }
            }
            
            print(payload_db)
            
            # Invocar la lambda con el lote
            invoke_lambda_db(payload_db, DB_ACCESS_LAMBDA_ARN)

    print(" Todos los puntos fueron subidos correctamente a S3.")
    
    
    ######################################################