import json
import boto3
import requests
import re
from datetime import datetime
from collections import defaultdict
from DB_capa_principal import db_upsert_capa_principal,db_update_habilitado_fase3 # type: ignore
from DB_fase1 import db_upsert_fase_1 #
from DB_fase3 import db_upsert_fase_3_a_data,db_upsert_fase_3_a_status,db_fase_3_a_b_trazabilidad_mediciones,db_update_trazabilidad
import os
import traceback


# traer las variables de entorno
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]
ENTREGABLES_FASE_X = os.getenv("ENTREGABLES_FASE_X", "").split(",")



# Definir el cliente de s3
s3 = boto3.client('s3')

# Definir el cliente de Lambda
lambda_client = boto3.client('lambda')
client_lambda_db = boto3.client("lambda", region_name="us-east-1") 



#ubicacion base de la carpeta en s3 donde se buscarán los puntos
base_attach_prefix = "ArcGIS-Data/Puntos/"

# cache para guardar los prefix ya encontrados por parent_id
prefix_cache = {}

# funcion para invocar lambda que genera los archivos
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
 
# funcion para invicar lambda de base de datos
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
        #print(access_token)
        return access_token
    else:
        print("error en la obtencion del token") 
   
# función que eemplaza espacios, guiones y caracteres no válidos por guiones bajos     
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
    #donde se almacena la lista de diccionarios por punto 
    feature_jsons = []
    
    
    try:
        
        forzar_informe = data["edits"][4].get("forzarInforme")
    except Exception as e:
        forzar_informe = "false"
    
    #Se recorre el payload recibido para extraer metadata
    for edit in data.get("edits", []):
        
        layer_id = edit.get("id")
        features = edit.get("features", {})
        geometria = features.get("geometry",{})
        

        for action in ["adds", "updates"]:
            
            for feat in features.get(action, []):
                dict_for_payload_fase_3_a_status = []
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
                    
                if layer_id == 3:
                    
                    dia_desinstalacion = attrs.get("CAMPO_EXTRA_9_TEXT")
                    hora_desinstalacion = attrs.get("CAMPO_EXTRA_8_TEXT")

                    # Validar que ambos valores existan y no estén vacíos
                    if dia_desinstalacion and hora_desinstalacion:
                        try:
                            # Convertir día a datetime
                            dt_dia = datetime.fromisoformat(dia_desinstalacion)

                            # Separar hora
                            h, m, s = map(int, hora_desinstalacion.split(":"))

                            # Combinar fecha y hora
                            fecha_hora_desinstalacion = dt_dia.replace(
                                hour=h,
                                minute=m,
                                second=s
                            )
                            
                            #Convertir a timestamp para INSERT (timestamptz)
                            fecha_hora_des = fecha_hora_desinstalacion.isoformat()

                            print("FECHA HORA DESINSTALACION:", fecha_hora_des)

                            # Guardar el valor combinado en el atributo
                            attrs = {
                                **attrs,
                                "CAMPO_EXTRA_8_TEXT": fecha_hora_des
                            }

                        except Exception as e:
                            print("Error al procesar fecha y hora:", e)
                            # (Opcional) no modificar nada si hay error
                    else:
                        fecha_hora_des = None
                        print("Día u hora vacíos → no se realiza combinación")

                    
                    datalogger_2 = attrs.get("CAMPO_EXTRA_1")
                    
                    
                    # Información del datalogger 1
                    dict_for_payload_fase_3_a_status.append({
                        "PARENT_ID" : global_fase,
                        "FID_ELEM" : attrs.get("FID_ELEM") ,
                        "IDENTIFICADOR_DATALOGGER" : attrs.get("IDENTIFICADOR_DATALOGGER"),#para cada datalogger
                        "TIPO_PUNTO": attrs.get("TIPO_PUNTO") ,
                        "EQUIPO__DATALOGGER_INSTALADOS" : attrs.get("EQUIPO__DATALOGGER_INSTALADOS"),
                        "CIRCUITO_ACU": attrs.get("CIRCUITO_ACU"),
                        "FECHA_FASE3" : attrs.get("FECHA_FASE3"),
                        "MEDIDA_PRESION":attrs.get("MEDIDA_PRESION"), #para cada datalogger
                        "MEDIDA_PRESION_2":attrs.get("MEDIDA_PRESION2"), #para cada datalogger
                        "MEDIDA_CAUDAL" :attrs.get("MEDIDA_CAUDAL"),
                        "MEDIDA_VELOCIDAD" : attrs.get("MEDIDA_VELOCIDAD"),
                        "MEDIDA_NIVEL" : attrs.get("MEDIDA_NIVEL"),
                        "REFERENCIA_PRESION" : attrs.get("REFERENCIA_PRESION"), #para cada datalogger
                        "REFERENCIA_PRESION_2" : attrs.get("MEDIDA_REF_PRESION_2"),
                        "CARGA_BATERIA" : attrs.get("CARGA_BATERIA"), #para cada datalogger
                        "VARIABLES_MEDICION" : attrs.get("VARIABLES_MEDICION"),  
                        "CAMPO_EXTRA_8_TEXT" : fecha_hora_des,
                        "CHECK_REC" : attrs.get("CHECK_REC"),
                        "TIEMPO_MUESTEREO" : attrs.get("TIEMPO_MUESTEREO"),
                        "CAMPO_EXTRA_7_LIST" : attrs.get("CAMPO_EXTRA_7_LIST"),
                        "CAMPO_EXTRA_6_TEXT" : attrs.get("CAMPO_EXTRA_6_TEXT"),
                        "SENSORES_LIMPIOS" : attrs.get("SENSORES_LIMPIOS"),
                        "ALMACENAMIENTO_DATOS_LOCALES" : attrs.get("ALMACENAMIENTO_DATOS_LOCALES"),
                        "TRANSMISION_DATOS" : attrs.get("TRANSMISION_DATOS")                      
                        })

                    # si hay un segundo datalogger agregar información
                    if datalogger_2 is not None and datalogger_2 != "":
                        dict_for_payload_fase_3_a_status.append({
                            "PARENT_ID" : global_fase,
                            "FID_ELEM" : attrs.get("FID_ELEM") ,
                            "IDENTIFICADOR_DATALOGGER" : datalogger_2,
                            "TIPO_PUNTO": attrs.get("TIPO_PUNTO") ,
                            "EQUIPO__DATALOGGER_INSTALADOS" : attrs.get("EQUIPO__DATALOGGER_INSTALADOS"),
                            "CIRCUITO_ACU": attrs.get("CIRCUITO_ACU"),
                            "FECHA_FASE3" : attrs.get("FECHA_FASE3"),
                            "MEDIDA_PRESION":attrs.get("MEDIDA_PRESION"), 
                            "MEDIDA_PRESION_2":attrs.get("MEDIDA_PRESION2"), 
                            "MEDIDA_CAUDAL" :attrs.get("MEDIDA_CAUDAL"),
                            "MEDIDA_VELOCIDAD" : attrs.get("MEDIDA_VELOCIDAD"),
                            "MEDIDA_NIVEL" : attrs.get("MEDIDA_NIVEL"),
                            "REFERENCIA_PRESION" : attrs.get("REFERENCIA_PRESION"),
                            "REFERENCIA_PRESION_2" : attrs.get("MEDIDA_REF_PRESION_2"),
                            "CARGA_BATERIA" : attrs.get("CAMPO_EXTRA_2"), 
                            "VARIABLES_MEDICION" : attrs.get("VARIABLES_MEDICION"),
                            "CAMPO_EXTRA_8_TEXT" : fecha_hora_des,
                            "CHECK_REC" : attrs.get("CHECK_REC"),
                            "TIEMPO_MUESTEREO" : attrs.get("TIEMPO_MUESTEREO"),
                            "CAMPO_EXTRA_7_LIST" : attrs.get("CAMPO_EXTRA_7_LIST"),
                            "CAMPO_EXTRA_6_TEXT" : attrs.get("CAMPO_EXTRA_6_TEXT"),
                            "SENSORES_LIMPIOS" : attrs.get("SENSORES_LIMPIOS"),
                            "ALMACENAMIENTO_DATOS_LOCALES" : attrs.get("ALMACENAMIENTO_DATOS_LOCALES"),
                            "TRANSMISION_DATOS" : attrs.get("TRANSMISION_DATOS")                       
                        }) 
                    
                identificador = attrs.get("OBJECTID") 
                point_type = attrs.get("TIPO_PUNTO")
                geometry = feat.get("geometry")
                circuito = attrs.get("CIRCUITO_ACU")
                
                # Payload que será utilizado para generar archivos
                if identificador:
                    feature_jsons.append({
                        "layer_id": layer_id,
                        "OBJECTID": identificador,
                        "geometry": geometry,
                        "attributes": attrs,
                        "point_type" : point_type,
                        "GlobalID"  : Global_ID,
                        "circuito" : circuito,
                        "GlobalID_Fase" : global_fase,
                        "forzarInforme" : forzar_informe,
                        "payload_fase_3_a_satus": dict_for_payload_fase_3_a_status
                        
                    })
                
                
                
                        
                        
                    
                    

    return feature_jsons



def lambda_handler(event, context):

    data = []
    payload_capa_principal = []
    payload_f1 = []
    payload_f2 = []
    payload_f3 = []
    
    # Diccionario donde cada key será un parent_id para generar payload que
    # invooca la lambda de generación de archivos
    capa_principal = defaultdict(list)
    fase_1 = defaultdict(list)
    fase_2 = defaultdict(list)
    fase_3 = defaultdict(list)
    db_fase_3_a_status = defaultdict(list)
 

    #diccionario para realcionar gobal id de padre con gobal id  de hijo:
    parents_relation = []
    # lista de parents ID de fase 3
    parents_ID_fase_3 = []
    
    ##PARA EXTRAER UPDATES POR PUNTO
    
    # 1. Obtener una lista de diccionarios con los updates
    data = event if isinstance(event, dict) else json.loads(event['body'])

    # 2. Se extrae una lista de diccionarios con la información de los puntos added 
    # y updated realizados en arcgis. La segunda lista corresponde a una lista de
    # diccionarios que será utilizada para la base de datos de fase 3: "fase_3_a_status"

    features = get_feature_jsons(data)
    print ("PUNTOS UPDATED", features)

    # 3. Extraer la información del diccionario
    for feature in features:
        
        layer_id = feature["layer_id"]
        point_type = feature["point_type"]
        circuito = feature["circuito"]
        GlobalID = feature["GlobalID"]
        atributos = feature["attributes"]
        geometria = feature["geometry"]
        GlobalID_fase = feature["GlobalID_Fase"]
        forzar_informe = feature["forzarInforme"]
        list_of_dict_fase_3_status = feature["payload_fase_3_a_satus"]
        #print("PAYLOAD input fase_3_a_status:",list_of_dict_fase_3_status)
        
        #Diccionario para relacionar global Id de padre e hijo
        if layer_id != 0:
            relation_ids = {
                "padre" : GlobalID,
                "hijo"  : GlobalID_fase
            }
            parents_relation.append(relation_ids)

        #Formato que se almacena en s3
        if layer_id == 0:
            atr_glob = {**atributos, "PARENT_ID": GlobalID}
            atr_geom = {**atr_glob,**geometria} 
        else:
            atr_geom = {**atributos, "PARENT_ID": GlobalID}
   
                   
        #Generar  timestamp
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        
        #Sanitizar los nombres para asignar nombre de archivo a guardar en s3
        safe_point_type = sanitize_name(point_type)
        safe_circuito = sanitize_name(circuito)
        filename = f"{GlobalID}__{safe_point_type}__{safe_circuito}__{timestamp}.json"
        
        #Carpeta principal de cada  punto
        base_prefix = f"ArcGIS-Data/Puntos/{safe_circuito}/{GlobalID}_{safe_point_type}/"
        
        
        #DETERMINAR DESTINO SEGUN ID
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
                "attachments": [],
                "forzarInforme": forzar_informe
                })
            
        elif layer_id == 2:
            key = f"{base_prefix}Fase2/{filename}"
            
            # Crear una entrada base para este punto sin attachments aun 
            fase_2[GlobalID].append({
                "payload": atr_geom,
                "attachments": [],
                "forzarInforme": forzar_informe
                })
            
        elif layer_id == 3:
            key = f"{base_prefix}Fase3/{filename}"
            
            #se agrega el parent ID por punto (para payload de base de datos
            # fase_3_a_b_trazabilidad_mediciones)
            parents_ID_fase_3.append(GlobalID)
            
            #Payload para generar archivos por punto
            # Crear una entrada base para este punto sin attachments aun 
            fase_3[GlobalID].append({
                "payload": atr_geom,
                "attachments": [],
                "forzarInforme": forzar_informe
                })
            
            
            for item in list_of_dict_fase_3_status:
                #Payload para base de datos fase_3_a_status
                db_fase_3_a_status[GlobalID].append({
                    "payload": item
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

        #print(f" Subido {key}")
    
    #print(parents_relation)
    ##Para los attachments##    
    
    # Extraer un diccionario con la informacion de los attachmens added y updated realizados en arcgis
    attachments = get_attachments(data)
    
    #Generar un token par acceder a la API de ARCGIS y traer los nuevos attachmnets
    token = http_token_request()
    
    # Mapeo de hijo y  padre (para almacenar en s3 relacionar attachments)
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
        
        #Para relacionar parent id de los atachments con el punto
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
                        #print(f"  Carpeta encontrada: {found_prefix}")
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
    payload_db_fase_3_a_status = dict(db_fase_3_a_status)



    # ============================================
    # GENERACIÓN DE PAYLOADS PARA LAMBDA DE BASE DE DATOS
    # ============================================

    try:

        json_capa_principal = json.dumps(payload_capa_principal)
        json_fase_1 = json.dumps(payload_fase_1)
        json_fase_3 = json.dumps(payload_fase_3)
        json_payload_db_fase_3_a_status = json.dumps(payload_db_fase_3_a_status)

        # --- CAPA PRINCIPAL ---
        payload_db_capa_principal = db_upsert_capa_principal(json_capa_principal)
        print("Payload capa principal:", payload_db_capa_principal)
        invoke_lambda_db(payload_db_capa_principal, DB_ACCESS_LAMBDA_ARN)

        # --- FASE 1 ---
        payload_db_fase_1 = db_upsert_fase_1(json_fase_1)
        print("Payload fase 1:", payload_db_fase_1)
        invoke_lambda_db(payload_db_fase_1, DB_ACCESS_LAMBDA_ARN)

        # --- Actualizar columna HABILITADO_FASE3 ---
        payload_update_habilitado_fase3 = db_update_habilitado_fase3(parents_relation)
        print("Payload col_Hab_Fase_3:", payload_update_habilitado_fase3)
        invoke_lambda_db(payload_update_habilitado_fase3, DB_ACCESS_LAMBDA_ARN)

        # --- FASE 3 A DATA ---
        payload_db_fase_3_a_data = db_upsert_fase_3_a_data(json_fase_3)
        #print("Payload fase 3 a data:", payload_db_fase_3_a_data)
        invoke_lambda_db(payload_db_fase_3_a_data, DB_ACCESS_LAMBDA_ARN)

        # --- FASE 3 A STATUS ---
        payload_db_fase_3_a_status = db_upsert_fase_3_a_status(json_payload_db_fase_3_a_status)
        #print("Payload fase 3 a status:", payload_db_fase_3_a_status)
        invoke_lambda_db(payload_db_fase_3_a_status, DB_ACCESS_LAMBDA_ARN)

        # --- TRAZABILIDAD & FASE 3 A B ---
        print("PARENTS ID FOR FASE 3:", parents_ID_fase_3)

        if parents_ID_fase_3:

            # FASE 3 A B TRAMO DE TRAZABILIDAD
            #db_fase_3_a_b_trazabilidad_mediciones(parents_ID_fase_3)
            print("")

            
    except Exception as e:
        print(" ERROR DURANTE LA EJECUCIÓN DE LA SECUENCIA DE UPDATES/INSERTS EN LA BASE DE DATOS")
        print("Detalles del error:", str(e))
        traceback.print_exc()

        
    # Generar los payloads para invocar lambda que genera archivos por punto

    for count2 in payload_fase_1:
        for payload_f1 in payload_fase_1[count2]:
            # En este punto payload_cp es un diccionario limpio, sin corchetes
            # Convertir a JSON
            json_f1 = json.dumps(payload_f1)
            #print("PAYLOAD_fase_1:",json_f1 )
            #invoke_lambda(payload_f1,0)
        
    for count3 in payload_fase_2:
        for payload_f2 in payload_fase_2[count3]:
            # En este punto payload_cp es un diccionario limpio, sin corchetes
            json_f2 = json.dumps(payload_f2)
            #print("PAYLOAD_fase_2:",json_f2 )
            #invoke_lambda(payload_f2,1)
            
    for count4 in payload_fase_3:
        for payload_f3 in payload_fase_3[count4]:
            # En este punto payload_cp es un diccionario limpio, sin corchetes
            json_f3 = json.dumps(payload_f3)
            print("PAYLOAD_fase_3:",json_f3 )
            #invoke_lambda(payload_f3,2)