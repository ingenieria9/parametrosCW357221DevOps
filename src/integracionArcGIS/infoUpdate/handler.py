import json
import boto3
import requests
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import os

# traer las variables de entorno
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]
#ENTREGABLES_FASE_X = os.environ["ENTREGABLES_FASE_X"]
ENTREGABLES_FASE_X = os.getenv("ENTREGABLES_FASE_X", "").split(",")



# Definir el cliente de s3
s3 = boto3.client('s3')
# Definir el cliente de Lambda
lambda_client = boto3.client('lambda')
client_lambda_db = boto3.client("lambda", region_name="us-east-1") 

# Diccionario donde cada key será un parent_id
capa_principal = defaultdict(list)
fase_1 = defaultdict(list)
fase_2 = defaultdict(list)
fase_3 = defaultdict(list)

#ubicacion base de la carpeta en s3 donde se buscarán los puntos
base_attach_prefix = "ArcGIS-Data/Puntos/"

# cache para guardar los prefix ya encontrados por parent_id
prefix_cache = {}

# funcion para invicar lambda que genera los archivos
def invoke_lambda(payload,fase):
    print("entregables arn")
    print(ENTREGABLES_FASE_X)
    print(ENTREGABLES_FASE_X[fase])
    response = lambda_client.invoke(
        FunctionName=ENTREGABLES_FASE_X[fase],
        InvocationType='Event',  # async
        Payload=json.dumps(payload).encode('utf-8')  #  convierte a JSON y luego a bytes
    )
    return response 
 
def invoke_lambda_db(payload, FunctionName):
    response = client_lambda_db.invoke(
        FunctionName=FunctionName,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )
    
    
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
    

# función para obtener diccionario de attachmets
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
                    parent = att.get("parentGlobalId")
                    # Eliminar cualquier paréntesis o llave (por si acaso)
                    parent_id = re.sub(r"[{}]", "", parent)
                    contentType = att.get("contentType")
                    attachment_id = att.get("attachmentId")

                    
                    #Diccionario con metadata del attachment
                    urls.append({
                        "layer_id"      : layer_id,
                        "parent_id"     : parent_id,
                        "contentType"   : contentType,
                        "url"           : url,
                        "attachment_id" : attachment_id
                    })
                   
    return urls
                                        
                                 
        
# función para obtener diccionario de los items agregados o actualizados en arcgis
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
        geometria = features.get("geometry",{})


        for action in ["adds", "updates"]:
            
            for feat in features.get(action, []):
                
                attrs = feat.get("attributes", {})
                
                if layer_id == 0:
                    Global = attrs.get("GlobalID")
                    # Eliminar cualquier paréntesis o llave (por si acaso)
                    Global_ID = re.sub(r"[{}]", "", Global)
                    global_fase = None
                else:
                    Global = attrs.get("PARENT_ID")
                    global_f = attrs.get("GlobalID")
                    # Eliminar cualquier paréntesis o llave (por si acaso)
                    Global_ID = re.sub(r"[{}]", "", Global)
                    global_fase = re.sub(r"[{}]", "", global_f)  
                    
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
                        "circuito" : circuito,
                        "GlobalID_Fase" : global_fase
                        
                    })

    return feature_jsons

def convertir_valores_fecha(data):
    """
    Convierte valores tipo timestamp (en milisegundos) a formato legible
    solo si la clave contiene la palabra 'fecha' (insensible a mayúsculas).
    Interpreta el timestamp como UTC y lo convierte a UTC-5.
    Funciona de forma recursiva para dicts y listas.
    """

    def convertir_fecha(key, valor):
        try:
            if "fecha" in key.lower():
                if isinstance(valor, (int, float)) or (isinstance(valor, str) and valor.isdigit()):
                    timestamp = int(valor) / 1000  # convertir a segundos
                    # Interpretar en UTC y convertir a UTC-5
                    dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    #dt_utc_minus_5 = dt_utc.astimezone(timezone(timedelta(hours=-5)))
                    #return dt_utc_minus_5.strftime("%Y-%m-%d %H:%M:%S")
                    return dt_utc.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Error al convertir fecha ({key}): {e}")
        return valor

    if isinstance(data, dict):
        nuevo_dict = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                nuevo_dict[k] = convertir_valores_fecha(v)
            else:
                nuevo_dict[k] = convertir_fecha(k, v)
        return nuevo_dict

    elif isinstance(data, list):
        return [convertir_valores_fecha(v) for v in data]

    else:
        return data

def build_bulk_upsert_sql(table_name: str, rows: list[dict], conflict_key: str = "PARENT_ID") -> str:
    """
    Construye un SQL con múltiples VALUES y un único ON CONFLICT.
    """
    if not rows:
        return ""

    # Asegurar que todas las filas tengan las mismas columnas
    columns = list(rows[0].keys())
    col_names = ", ".join([f'"{c}"' for c in columns])

    values_sql = []
    for row in rows:
        vals = []
        for c in columns:
            v = row.get(c, "NULL")
            if str(v).upper() == "NULL":
                vals.append("NULL")
            elif isinstance(v, str):
                vals.append(f"'{v}'")
            else:
                vals.append(str(v))
        values_sql.append(f"({', '.join(vals)})")

    update_clause = ", ".join(
        [f'"{c}" = EXCLUDED."{c}"' for c in columns if c != conflict_key]
    )

    sql = f"""
    INSERT INTO "{table_name}" ({col_names})
    VALUES
        {', '.join(values_sql)}
    ON CONFLICT ("{conflict_key}")
    DO UPDATE SET
        {update_clause};
    """.strip()

    return sql

def db_upsert_capa_principal(json_data):
    #print(json_data)
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    puntos_capa_principal_fields = ["GlobalID","TIPO_PUNTO","FECHA_CREACION","FECHA_EDICION","CIRCUITO_ACU","SUBCIRCUIT_ACU",
                                "CUENCA_ALC","FID_ELEM","DIRECCION_ACU","CODIGO_CAJA_ACU","x","y","PUNTO_EXISTENTE","IPID_ELEM_ACU",
                                "IPID_ALC","FASE_INICIAL","VARIABLE_A_MEDIR"]

    all_rows = []

    #Caso 1: lista de objetos [{"id": {...}}, {"id": {...}}]
    if isinstance(json_data, list):
        items = []
        for entry in json_data:
            if isinstance(entry, dict):
                # cada elemento tiene una sola clave (ej: "263263636")
                for _, value in entry.items():
                    items.append(value)
        json_data = {str(i): v for i, v in enumerate(items)}  # convertir a dict uniforme

    #Caso 2: un único objeto con "attributes"
    elif "attributes" in json_data:
        json_data = {"single": json_data}

    #Caso 3: dict con múltiples claves
    for key, items in json_data.items():
        try:
            for item in items:  # porque item es una lista con objetos
                payload = item.get("payload", {})
                attributes = payload  # tus campos están dentro de payload

                attributes = convertir_valores_fecha(attributes)
                capa_principal_values = {}

                for field in puntos_capa_principal_fields:
                    value = attributes.get(field, None)
                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        value = str(value)
                    capa_principal_values[field] = value

                all_rows.append(capa_principal_values)
        except Exception as e:
            print(f"Error procesando registro {key}: {e}")


    upsert_sql = build_bulk_upsert_sql("puntos_capa_principal", all_rows, "GlobalID")

    print("upsert capa principal", upsert_sql)

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }

    invoke_lambda_db(payload_db, DB_ACCESS_LAMBDA_ARN)

def db_upsert_fase_1(json_data):

    #print(json_data)
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_1_fields = [
        "PARENT_ID", "TIPO_PUNTO", "FECHA_CREACION", "FECHA_EDICION",
        "condicion_fisica_general", "conexiones_hidraulicas", "UBICACION_GEO_CRITICA",
        "habilitado_medicion", "actualizacion_ubicacion", "cobertura",
        "requiere_instalacion_tapa", "requiere_limpieza", "FID_ELEM",
        "REQUIERE_FASE1", "PUNTO_ENCONTRADO", "EXPOSICION_FRAUDE", "FECHA_FASE1"
    ]

    all_rows = []

    #Caso 1: lista de objetos [{"id": {...}}, {"id": {...}}]
    if isinstance(json_data, list):
        items = []
        for entry in json_data:
            if isinstance(entry, dict):
                # cada elemento tiene una sola clave (ej: "263263636")
                for _, value in entry.items():
                    items.append(value)
        json_data = {str(i): v for i, v in enumerate(items)}  # convertir a dict uniforme

    #Caso 2: un único objeto con "attributes"
    elif "attributes" in json_data:
        json_data = {"single": json_data}

    #Caso 3: dict con múltiples claves
    for key, items in json_data.items():
        try:
            # Si cada clave tiene una lista de objetos, iterar sobre ella
            if isinstance(items, list):
                iterable = items
            else:
                iterable = [items]

            for item in iterable:
                payload = item.get("payload", {})
                attributes = payload.get("attributes", payload)  # usar payload directo si no hay 'attributes'

                attributes = convertir_valores_fecha(attributes)
                fase_1_values = {}

                for field in fase_1_fields:
                    value = attributes.get(field, None)
                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        # Normalizar valores tipo Sí/No -> 1/0
                        if field not in ["REQUIERE_FASE1", "PUNTO_ENCONTRADO"]:
                            if isinstance(value, str):
                                if value.strip().lower() == "si":
                                    value = 1
                                elif value.strip().lower() == "no":
                                    value = 0
                        value = str(value)
                    fase_1_values[field] = value

                # condicion_fisica_general
                if (
                    attributes.get("SIGNOS_DESGASTE_ACU") in ["Si", 1]
                    or attributes.get("DANOS_ESTRUCT_ACU") in ["Si", 1]
                    or attributes.get("TAPA_ASEGURADA_ACU") in ["No", 0]
                ):
                    fase_1_values["condicion_fisica_general"] = 0
                else:
                    fase_1_values["condicion_fisica_general"] = 1

                # conexiones_hidraulicas 
                if (attributes.get("ESTADO_OPTIMO_CON_HID_ACU") in ["No", 0] or
                    attributes.get("ESTADO_ADECUADO_TUBERIA_ACU") in ["No", 0] or
                    attributes.get("VALVULA_FUNCIONAL_ACU") in ["No", 0] or
                    attributes.get("PRESENTA_FUGAS_ACU") in ["Si", 1] or
                    attributes.get("FLUJO_DE_AGUA_ACU") in ["No", 0] or
                    attributes.get("VERIFICA_CONEX_ROSCADA_ACU") in ["No", 0] or
                    attributes.get("CUMPLE_MEDIDAS_MIN_MED_CAU_ACU") in ["No", 0]):
                    fase_1_values["conexiones_hidraulicas"] = 0
                else:
                    fase_1_values["conexiones_hidraulicas"] = 1

                # habilitado_medicion
                if (
                    attributes.get("PUNTO_REQUIERE_FASE2") in ["Si", 1]
                    and attributes.get("PUNTOS_HABILITADO_FASE3") in ["No", 0]
                ):
                    fase_1_values["habilitado_medicion"] = 0
                elif (
                    attributes.get("PUNTOS_HABILITADO_FASE3") in ["Si", 1]
                    and attributes.get("PUNTO_REQUIERE_FASE2") in ["No", 0]
                ):
                    fase_1_values["habilitado_medicion"] = 1

                all_rows.append(fase_1_values)

        except Exception as e:
            print(f"Error procesando registro {key}: {e}")

    upsert_sql = build_bulk_upsert_sql("fase_1", all_rows, "PARENT_ID")

    print("upsert_fase_1", upsert_sql)

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }

    invoke_lambda_db(payload_db, DB_ACCESS_LAMBDA_ARN)

def lambda_handler(event, context):

    #diccionario de gobal id de padre e hijo:
    parents_relation = []
    #Obtener un diccionario tipo python de la información recibida
    data = event if isinstance(event, dict) else json.loads(event['body'])

    #Se extrae un diccionario con la información de los puntos added y updated realizados en arcgis
    features = get_feature_jsons(data)

    #Extraer la información del diccionario
    for feature in features:
        
        layer_id = feature["layer_id"]
        point_type = feature["point_type"]
        circuito = feature["circuito"]
        GlobalID = feature["GlobalID"]
        atributos = feature["attributes"]
        geometria = feature["geometry"]
        GlobalID_fase = feature["GlobalID_Fase"]
        
        #NUEVO
        if layer_id != 0:
            relation_ids = {
                "padre" : GlobalID,
                "hijo"  : GlobalID_fase
            }
            parents_relation.append(relation_ids)

        #formato que se almacena en s3
        if layer_id == 0:
            atr_glob = {**atributos, "PARENT_ID": GlobalID}
            atr_geom = {**atr_glob,**geometria} 
        else:
            atr_geom = {**atributos, "PARENT_ID": GlobalID}
   
                   
            
       
        #Generar el nombre del archivo con un timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        #Sanitizar los nombres
        safe_point_type = sanitize_name(point_type)
        safe_circuito = sanitize_name(circuito)
        filename = f"{GlobalID}__{safe_point_type}__{safe_circuito}__{timestamp}.json"
        
        #Carpeta principal del punto
        base_prefix = f"ArcGIS-Data/Puntos/{safe_circuito}/{GlobalID}_{safe_point_type}/"
        
        
        #Determinar destino segun id
        if layer_id == 0:
            key = f"{base_prefix}Capa_principal/{filename}"
            
            # Crear una entrada base para este punto sin attachments aun
            capa_principal[GlobalID].append({
                "payload": atr_geom,
                "attachments": []
                })
            
        elif layer_id == 1:
            key = f"{base_prefix}Fase1/{filename}"
            
            # Crear una entrada base para este punto sin attachments aun
            fase_1[GlobalID].append({
                "payload": atr_geom,
                "attachments": []
                })
            
        elif layer_id == 2:
            key = f"{base_prefix}Fase2/{filename}"
            
            # Crear una entrada base para este punto sin attachments aun
            fase_2[GlobalID].append({
                "payload": atr_geom,
                "attachments": []
                })
            
        elif layer_id == 3:
            key = f"{base_prefix}Fase3/{filename}"
            
            # Crear una entrada base para este punto sin attachments aun
            fase_3[GlobalID].append({
                "payload": atr_geom,
                "attachments": []
                })
            
        else:
            continue  # ignora ids fuera de rango

        #Convertir a JSON
        json_data = json.dumps(atr_geom, indent=2, ensure_ascii=False)

        #Subir a S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json_data,
            ContentType="application/json"
        )

        print(f" Subido {key}")
    
    print(parents_relation)
    ##Para los attachments##    
    
    # Extraer un diccionario con la informacion de los attachmens added y updated realizados en arcgis
    attachments = get_attachments(data)
    
    #Generar un token par acceder a la API de ARCGIS y traer los nuevos attachmnets
    token = http_token_request()
    
    # Mapeo de hijo y  padre
    relaciones = {item["hijo"]: item["padre"] for item in parents_relation}     
    
    #Para extraer metadata, obtener la imagen y guardar en S3
    for attach in attachments:
        
        layer_id      = attach["layer_id"]
        contentType   = attach["contentType"]
        #parent id corresponde al global id de la fase 
        parent_id     = attach["parent_id"]
        attachment_id = attach["attachment_id"]
        
        #Identificar el formato de la imagen
        contentType_parts = contentType.split('/')
        imageType = contentType_parts[1]
        url = attach["url"]
        
        ###nuevo
        if layer_id != 0:
            hijo = attach["parent_id"]
            if hijo in relaciones:
                parent_id = relaciones[hijo]
            
        
        
    # Obtener (o buscar) la carpeta en S3 para cada parent_id ---
        if parent_id in prefix_cache:
            found_prefix = prefix_cache[parent_id]
           
        else:
            found_prefix = None
    
            # Buscar la carpeta en los circuitos dentro de "Puntos/"
            response = s3.list_objects_v2(
                Bucket=BUCKET_NAME, Prefix=base_attach_prefix, Delimiter='/'
            )
            
            for prefix_info in response.get('CommonPrefixes', []):
                prefix = prefix_info['Prefix']
                sub_response = s3.list_objects_v2(
                    Bucket=BUCKET_NAME, Prefix=prefix, Delimiter='/'
                )
                for sub_prefix_info in sub_response.get('CommonPrefixes', []):
                    sub_prefix = sub_prefix_info['Prefix']
                    pattern = rf"Puntos/.*/({parent_id})_"
                    if re.search(pattern, sub_prefix):
                        found_prefix = sub_prefix
                        print(f"  Carpeta encontrada: {found_prefix}")
                        break
                if found_prefix:
                    break

            if not found_prefix:
                print(f" No se encontró carpeta para GlobalID {parent_id}")
                
            prefix_cache[parent_id] = found_prefix  # cachear resultado 
            
    # Si no hay carpeta, saltar este attachment 
        if not found_prefix:
            continue

        # Determinar la ruta de destino según el layer_id 
        if layer_id == 0:
            
            attach_key = f"{found_prefix}Capa_principal/attachment__{parent_id}_{attachment_id}.{imageType}"
            
            if parent_id not in capa_principal:
                capa_principal[parent_id] = [{"payload": {}, "attachments": []}]
            capa_principal[parent_id][-1]["attachments"].append(attach_key)
            
        elif layer_id == 1:
            
            attach_key = f"{found_prefix}Fase1/attachment__{parent_id}_{attachment_id}.{imageType}"
            
            if parent_id not in fase_1:
                fase_1[parent_id] = [{"payload": {}, "attachments": []}]
            fase_1[parent_id][-1]["attachments"].append(attach_key)
            
        elif layer_id == 2:
            
            attach_key = f"{found_prefix}Fase2/attachment__{parent_id}_{attachment_id}.{imageType}"
            
            if parent_id not in fase_2:
                fase_2[parent_id] = [{"payload": {}, "attachments": []}]
            fase_2[parent_id][-1]["attachments"].append(attach_key)
            
        
        elif layer_id == 3:
            
            attach_key = f"{found_prefix}Fase3/attachment__{parent_id}_{attachment_id}.{imageType}"
            
            if parent_id not in fase_3:
                fase_3[parent_id] = [{"payload": {}, "attachments": []}]
            fase_3[parent_id][-1]["attachments"].append(attach_key)
        
        else:
            continue

        #print(f" Subiendo a S3 {attach_key}") 
               
   
        # Descargar la imagen
        response_result = requests.get(url, params={"token": token})
        

        # Subir el archivo a S3
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=attach_key,
            Body=response_result.content,
            ContentType=contentType
        )
        
        #print("Archivo subido correctamente a:", attach_key)
        
                
    payload_capa_principal = dict(capa_principal)
    payload_fase_1 = dict(fase_1)
    payload_fase_2 = dict(fase_2)
    payload_fase_3 = dict(fase_3)

    #Generar payloads para lambda de base de datos
    
    json_capa_principal = json.dumps(payload_capa_principal)
    json_fase_1 = json.dumps(payload_fase_1)
    
    print("capa principal")
    db_upsert_capa_principal(json_capa_principal)
    print("fase 1")
    db_upsert_fase_1(json_fase_1)    
   
   # Generar los payloads para invocar lambda que genera archivos por punto
    #for count1 in payload_capa_principal:
        ##print("PAYLOAD_capa_principal:", payload_cp)
            #invoke_lambda(payload_cp)

    for count2 in payload_fase_1:
        for payload_f1 in payload_fase_1[count2]:
            # En este punto payload_cp es un diccionario limpio, sin corchetes
            # Convertir a JSON
            json_f1 = json.dumps(payload_f1)
            print("PAYLOAD_fase_1:",json_f1 )
            invoke_lambda(payload_f1,0)
        
    for count3 in payload_fase_2:
        for payload_f2 in payload_fase_2[count3]:
            # En este punto payload_cp es un diccionario limpio, sin corchetes
            json_f2 = json.dumps(payload_f2)
            print("PAYLOAD_fase_2:",json_f2 )
            #invoke_lambda(payload_f2,1)
            
    for count4 in payload_fase_3:
        for payload_f3 in payload_fase_3[count4]:
            # En este punto payload_cp es un diccionario limpio, sin corchetes
            json_f3 = json.dumps(payload_f3)
            print("PAYLOAD_fase_3:",json_f3 )
            #invoke_lambda(payload_f3,2)