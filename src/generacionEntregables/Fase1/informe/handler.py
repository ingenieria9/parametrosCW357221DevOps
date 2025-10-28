'''
{
   "payload":{
      "layer_id":1,
      "OBJECTID":"0002",
      "geometry":"null",
      "attributes":{
         "OBJECTID":"0002",
         "GlobalID":"3CFDE950-8AE7-440E-B1E7-310C56A35794",
         "Creador":"central_ti_telemetrik",
         "Fecha_Creacion":1758818476306,
         "Editor":"central_ti_telemetrik",
         "Fecha_Edicion":1758829344252,
         "id" : "0002",
         "TIPO_PUNTO" : "puntos_medicion",
         "signos_desgaste" : "Si",
         "fugas" : "Si",
         "danios" : "No",
         "requiere_instalacion_tapa" : "Si",
         "requiere_limpieza" : "No",
         "razon_limpieza" : "",
         "requiere_clausura" : "",
         "comentario_cond_fisica" : "Tiene una pequeña fuga y desgaste en la tapa",
         "estado_conexion" : "Si",
         "estado_tuberia" : "Si",
         "accesorios_existentes" : "Si",
         "valvula_abre" : "Si",
         "valvula_cierra" : "Si",
         "flujo_agua" : "Si",
         "comentario_conexiones_hid" : "oK",
         "ubicacion_geografica_critica" : "No",
         "posible_expos_fraude" : "No",
         "comentario_vuln" : "Ok",
         "verificacion_4g" : "Si",
         "operador_4g" : "Claro",
         "equipos_usados" : "",
         "conclusiones" : "",
         "recomendaciones" : "Se debe corregir fuga y reemplazar tapa",
         "comentario_general" : "",
         "fecha_edicion" : "1758818476306",
         "actualizacion_ubicacion" : "No",
         "fecha_creacion" : "1758818476306",
         "latitud" : "37.21",
         "longitud" : "-72.912"
      },
      "point_type":"cajas_medicion"
   },
   "attachments":[
      "files/temp-image-folder/ejemplo1.jpg",
      "files/temp-image-folder/ejemplo2.jpg",
      "files/temp-image-folder/ejemplo3.jpg",
      "files/temp-image-folder/ejemplo4.jpg",
      "files/temp-image-folder/ejemplo5.jpg",
      "files/temp-image-folder/ejemplo6.jpg",
      "files/temp-image-folder/ejemplo7.jpg",
      "files/temp-image-folder/ejemplo8.jpg"
   ]
} 

Variables que vienen de la capa principal y no del payload:
circuito, subcircuito, cuenca, direccion_referencia, vrp
"circuito" : "tmk",
"direccion_referencia" : "Cra 42 #2 cerca al mall",
"vrp" : "vrp-0001",

key s3 de capa principal 
ArcGIS-Data/Puntos/{ID}_{TIPO_PUNTO}/Capa_principal/{latest-timestamp}.json
'''


import boto3
import json
from pathlib import Path
from docxtpl import DocxTemplate, InlineImage
from datetime import datetime
from docx.shared import Cm
import os
import requests


client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

s3 = boto3.client("s3")
TMP_DIR = Path("/tmp")

bucket_name = os.environ['BUCKET_NAME']
template_path_s3 = "files/plantillas/Fase1/"
output_path_s3 = "files/entregables/Fase1/"

template_name = {"circuito": "informe-acueducto.docx",
                 "cuenca": "informe-alcantarillado.docx"}

#COD_name = {"circuito": "ACU/MPH-EJ-06-01-F01-ACU-DIA-", "cuenca": "ALC/MPH-EJ-06-01-F01-ALC-DIA-"}

COD_name = {"circuito": "ACU/CIR/MPH-EJ-0601-{COD}-F01-ACU-DIA",
            "cuenca": "ALC/CUE/MPH-EJ-0601-{COD}-F01-ALC-DIA"}


def lambda_handler(event, context):   
    payload_data = event["payload"]["attributes"]
    TIPO_PUNTO = event["payload"]["attributes"]["TIPO_PUNTO"]
    FID_ELEM = event["payload"]["attributes"]["FID_ELEM"]
    GlobalID = event["payload"]["attributes"]["PARENT_ID"]

    capa_principal_data = obtener_info_de_capa_principal(bucket_name, TIPO_PUNTO, GlobalID)

    if TIPO_PUNTO == "vrp" or TIPO_PUNTO == "puntos_medicion":
        circuito_cuenca_valor = capa_principal_data.get("CIRCUITO_ACU", "N/A")
        circuito_cuenca = "circuito"
    else:
        circuito_cuenca_valor = capa_principal_data.get("CUENCA_ALC", "N/A")
        circuito_cuenca = "cuenca"

    print(circuito_cuenca, circuito_cuenca_valor)

    template_key = template_path_s3 + template_name.get(circuito_cuenca)

    # Paths locales en Lambda (/tmp)
    template_path = TMP_DIR / "plantilla.docx"
    output_path = TMP_DIR / "output.docx"

    # Descargar archivos desde S3
    s3.download_file(bucket_name, template_key, str(template_path))

    response = obtener_imagenes_grafana(circuito_cuenca_valor)
    #obtener imagenes grafana 
    img_path = "/tmp/grafico_fase_siguiente_puntos.png"
    with open(img_path, "wb") as f:
        f.write(response.content)

    contexto_general = build_general_context(circuito_cuenca_valor, circuito_cuenca)

    print("contexto_general", contexto_general)

    contexto_puntos = build_puntos_context(circuito_cuenca_valor, circuito_cuenca)

    print("contexto_puntos", contexto_puntos)

    imagenes ={
    "grafico_fase_siguiente_puntos": InlineImage(doc, img_path)
    }
    

    contexto = {**contexto_general,  "puntos": contexto_puntos, **imagenes}
    print(contexto)
    
    doc = DocxTemplate(template_path)
    doc.render(contexto)
    doc.save(output_path)


    # Subir resultado a S3
    #obtener de S3 el archivo json que contiene el codigo del circuito para construir el archivo
    # Descargar temporalmente en /tmp
    tmp_path_code = TMP_DIR / "code.json"
    if TIPO_PUNTO == "camara":
        code_file = "files/epm_codes/CODE_ALC_CUE.json"
    else:
        code_file = "files/epm_codes/CODE_ACU_CIR.json"
    s3.download_file(bucket_name, code_file, str(tmp_path_code))

    # Leer el contenido con validación
    with open(tmp_path_code, "r", encoding="utf-8") as f:
        contenido = f.read().strip()
        if not contenido:
            print(" El archivo JSON está vacío.")
        try:
            code_json =  json.loads(contenido)
        except json.JSONDecodeError as e:
            print(f" Error al parsear JSON {code_file}: {e}")

    code_data = code_json[circuito_cuenca_valor]

    if not code_data:
        code_data = circuito_cuenca_valor

    file_name = COD_name[circuito_cuenca].format(COD=code_data)

    output_key = f"{output_path_s3}{file_name}.docx"
    s3.upload_file(str(output_path), bucket_name, output_key)
    
    return {
        "status": "ok",
        "output_file": f"s3://{bucket_name}/{output_key}"
    }

def obtener_imagenes_grafana(circuito):
    response = "hello"
    return response
    

def obtener_info_de_capa_principal(bucket_name, TIPO_PUNTO, GlobalID):
    # Construir el prefijo correcto
    s3_key_capa_principal = (
        f"ArcGIS-Data/Puntos/{GlobalID}_{TIPO_PUNTO}/Capa_principal/"
    )

    # Listar objetos en esa carpeta
    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_key_capa_principal)

    if "Contents" not in s3_objects:
        print("No hay archivos en la carpeta Capa_principal.")
        return {}

    # Filtrar SOLO archivos que terminen en .json
    json_files = [
        obj for obj in s3_objects["Contents"]
        if obj["Key"].lower().endswith(".json")
    ]

    if not json_files:
        print("No se encontraron archivos .json en Capa_principal.")
        print(f"Se buscó usando Prefix: {s3_key_capa_principal}")
        return {}

    # Tomar el más reciente por fecha
    latest_json = max(json_files, key=lambda x: x["LastModified"])["Key"]

    print(f"Usando archivo principal: {latest_json}")

    # Descargar temporalmente en /tmp
    tmp_path = TMP_DIR / "capa_principal.json"
    s3.download_file(bucket_name, latest_json, str(tmp_path))

    # Leer el contenido con validación
    with open(tmp_path, "r", encoding="utf-8") as f:
        contenido = f.read().strip()
        if not contenido:
            print(" El archivo JSON está vacío.")
            return {}
        try:
            return json.loads(contenido)
        except json.JSONDecodeError as e:
            print(f" Error al parsear JSON {latest_json}: {e}")
            return {}


def build_puntos_context(circuito_cuenca_valor, circuito_cuenca):
    if circuito_cuenca == "circuito":
        circuito_cuenca = "CIRCUITO_ACU"
    else: 
        circuito_cuenca = "CUENCA_ALC"

    # 1. Obtener lista de id y TIPO_PUNTO
    query = f"""
        SELECT "GlobalID", TIPO_PUNTO 
        FROM puntos_capa_principal 
        WHERE "{circuito_cuenca}" = '{circuito_cuenca_valor}'
    """
    print(query)
    resultados = query_db(query, "fecha_creacion")

    # Extraer listas de id y TIPO_PUNTO
    lista_id = [row["GlobalID"] for row in resultados]
    lista_tipo = [row["TIPO_PUNTO"] for row in resultados]

    print(lista_id)
    print(lista_tipo)

    puntos_visitados_consolidados = []

    for punto, TIPO_PUNTO in zip(lista_id, lista_tipo):
        key_s3_prefix = f"ArcGIS-Data/Puntos/{punto}_{TIPO_PUNTO}/Fase1/"
        s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=key_s3_prefix)

        # Filtrar archivos JSON
        json_files = [
            obj for obj in s3_objects.get("Contents", [])
            if obj["Key"].endswith(".json")
        ]

        if not json_files:
            continue  # No hay archivos para este punto, saltamos

        # 2. Seleccionar el archivo más reciente por fecha de modificación
        json_files.sort(key=lambda x: x["LastModified"], reverse=True)
        latest_key = json_files[0]["Key"]

        # 3. Descargar y cargar el contenido JSON
        obj_s3 = s3.get_object(Bucket=bucket_name, Key=latest_key)
        json_data = json.loads(obj_s3["Body"].read().decode("utf-8"))

        # 4. Extraer solo los atributos deseados
        atributos_deseados = [
            "FID_ELEM",
            "COMENT_COND_FISICA_ACU",
            "COMENT_GENERAL_CONEX_HIDRAU_ACU",
            "Comentarios_adicionales_acerca_",
            "Conclusiones",
            "Recomendaciones",
            "PUNTOS_HABILITADO_FASE3"
        ]

        punto_filtrado = {
            "TIPO_PUNTO": TIPO_PUNTO,
            "archivo_s3": latest_key
        }

        # Validamos que 'attributes' exista (info esta en attributes)
        data_attr = json_data.get("attributes", {})
        print(data_attr)

        for attr in atributos_deseados:
            punto_filtrado[attr] = data_attr.get(attr)

        puntos_visitados_consolidados.append(punto_filtrado)

    return puntos_visitados_consolidados


def build_general_context(circuito_cuenca_valor, circuito_cuenca):

    if circuito_cuenca == "circuito":
        data = get_general_data_circuito(circuito_cuenca_valor)
    else:   
        data = {}  # Implementar función similar para cuenca 

    print(data)
    
    campos_contexto = list(data.keys())

    # Construir contexto final
    context = {
        campo: data.get(campo, "")
        for campo in data.keys()
    }

    return context



def get_general_data_circuito(circuito):
    query = f"""
    WITH puntos_filtrados AS (
        SELECT "GlobalID", "TIPO_PUNTO"
        FROM puntos_capa_principal
        WHERE "CIRCUITO_ACU" = '{circuito}'
    )

    SELECT
        -- Totales por tipo desde puntos_capa_principal
        (SELECT COUNT(*) 
        FROM puntos_capa_principal 
        WHERE "CIRCUITO_ACU" = '{circuito}'
        AND "TIPO_PUNTO" = 'puntos_medicion') AS numero_puntos_medicion_totales,

        (SELECT COUNT(*) 
        FROM puntos_capa_principal 
        WHERE "CIRCUITO_ACU" = '{circuito}'
        AND "TIPO_PUNTO" = 'vrp') AS numero_vrp_totales,

        -- Visitas en fase_1 (JOIN con ids del circuito)
        COUNT(DISTINCT CASE WHEN p."TIPO_PUNTO" = 'puntos_medicion' THEN f."FID_ELEM" END) AS numero_puntos_medicion_visitadas,
        COUNT(DISTINCT CASE WHEN p."TIPO_PUNTO" = 'vrp' THEN f."FID_ELEM" END) AS numero_vrp_visitadas,
        
        -- Fechas mínima y máxima


        -- Vulnerables
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'puntos_medicion' AND f.vulnerabilidades = 1) AS puntos_vulnerables,
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'vrp' AND f.vulnerabilidades = 1) AS vrp_vulnerables,


        -- Condición física OK
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'puntos_medicion' AND f.condicion_fisica_general = 0) AS puntos_cond_ok,
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'vrp' AND f.condicion_fisica_general = 0) AS vrp_cond_ok,

        -- Conexiones hidráulicas OK
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'puntos_medicion' AND f.conexiones_hidraulicas = 0) AS puntos_hid_ok,
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'vrp' AND f.conexiones_hidraulicas = 0) AS vrp_hid_ok,

        -- Habilitado medición OK
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'puntos_medicion' AND f.habilitado_medicion = 1) AS puntos_ok,
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'vrp' AND f.habilitado_medicion = 1) AS vrp_ok,

        -- Instalación tapa
        COUNT(*) FILTER (WHERE p."TIPO_PUNTO" = 'puntos_medicion' AND f.requiere_instalacion_tapa = 1) AS puntos_tapa

    FROM fase_1 f
    INNER JOIN puntos_filtrados p ON f."PARENT_ID"  = p."GlobalID";
    """

    # --- UNA SOLA LLAMADA A LA LAMBDA ---
    result = query_db(query, "fecha_creacion")[0]
    print(result)

    #fecha primera visita 
    query_primera_visita = f"""
    WITH puntos_filtrados AS (
    SELECT "GlobalID", "TIPO_PUNTO"
    FROM puntos_capa_principal
    WHERE "CIRCUITO_ACU" = '{circuito}'
    )
    SELECT
        MIN(f."FECHA_EDICION") AS fecha_primera_visita
    FROM fase_1 f
    INNER JOIN puntos_filtrados p ON f."PARENT_ID" = P."GlobalID";"""

    fecha_primera_visita = query_db(query_primera_visita, "fecha_primera_visita")[0]["fecha_primera_visita"]
    print(fecha_primera_visita)

    #fecha ultima visita 
    query_ultima_visita = f"""
    WITH puntos_filtrados AS (
    SELECT "GlobalID", "TIPO_PUNTO"
    FROM puntos_capa_principal
    WHERE "CIRCUITO_ACU" = '{circuito}'
    )
    SELECT
        MAX(f."FECHA_EDICION") AS fecha_ultima_visita
    FROM fase_1 f
    INNER JOIN puntos_filtrados p ON f."PARENT_ID" = p."GlobalID";"""

    fecha_ultima_visita = query_db(query_ultima_visita, "fecha_ultima_visita")[0]["fecha_ultima_visita"]
    print(fecha_ultima_visita)


    # Procesamiento de fechas
    #fecha_primera_visita_ts = result["fecha_primera_visita"]
    #fecha_ultima_visita_ts = result["fecha_ultima_visita"]

    #calculo-dias
    formato = "%Y-%m-%d %H:%M:%S"

    if fecha_primera_visita and fecha_ultima_visita:
        try:
            f_min = datetime.strptime(fecha_primera_visita, formato)
            f_max = datetime.strptime(fecha_ultima_visita, formato)

            diferencia = f_max - f_min
            total_dias = round(diferencia.total_seconds() / 86400)  # 86400 segundos en un día

        except Exception as e:
            print(f"Error al procesar fechas: {e}")
            total_dias = 0
    else:
        total_dias = 0

    # Otros cálculos derivados
    numero_puntos_medicion_visitadas = result["numero_puntos_medicion_visitadas"]
    numero_vrp_visitadas = result["numero_vrp_visitadas"]

    numero_total_visitas = numero_puntos_medicion_visitadas + numero_vrp_visitadas
    puntos_intervencion = numero_puntos_medicion_visitadas - result["puntos_ok"]
    vrp_intervencion = numero_vrp_visitadas - result["vrp_ok"]

    fecha_primera_visita_date = formatear_fecha(fecha_primera_visita)
    fecha_ultima_visita_date = formatear_fecha(fecha_ultima_visita)
    
    return {
        "nombre_circuito": circuito,
        "numero_puntos_medicion_totales": result["numero_puntos_medicion_totales"],
        "numero_vrp_totales": result["numero_vrp_totales"],
        "numero_puntos_medicion_visitadas": numero_puntos_medicion_visitadas,
        "numero_vrp_visitadas": numero_vrp_visitadas,
        "fecha_primera_visita": fecha_primera_visita_date,
        "fecha_ultima_visita": fecha_ultima_visita_date,
        "total_dias": total_dias,
        "numero_total_visitas": numero_total_visitas,
        "puntos_ok": result["puntos_ok"],
        "vrp_ok": result["vrp_ok"],
        "puntos_vulnerables": result["puntos_vulnerables"],
        "vrp_vulnerables": result["vrp_vulnerables"],
        "puntos_clausurar": result["puntos_clausurar"],
        "vrp_clausurar": result["vrp_clausurar"],
        "puntos_cond_ok": result["puntos_cond_ok"],
        "vrp_cond_ok": result["vrp_cond_ok"],
        "puntos_hid_ok": result["puntos_hid_ok"],
        "vrp_hid_ok": result["vrp_hid_ok"],
        "puntos_intervencion": puntos_intervencion,
        "vrp_intervencion": vrp_intervencion,
        "puntos_tapa": result["puntos_tapa"]
    }

def formatear_fecha(fecha_str):
    if not fecha_str:
        return ""
    try:
        fecha_dt = datetime.strptime(fecha_str, "%Y-%m-%d %H:%M:%S")
        return fecha_dt.strftime("%Y-%m-%d")  # Solo fecha
    except Exception as e:
        print(f"Error al formatear fecha: {e}")
        return ""

def query_db(query, time_column):
    payload_db = {
        "queryStringParameters": {
            "query": query,
            "time_column": time_column,
            "db_name": "parametros"
        }
    }
    response_db = invoke_lambda_db(payload_db, db_access_arn)
    body = json.loads(response_db["body"])
    print(body)
    return body


def invoke_lambda_db(payload, FunctionName):
    response = client_lambda_db.invoke(
        FunctionName=FunctionName,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )

    result = response["Payload"].read().decode("utf-8")

    try:
        return json.loads(result)
    except json.JSONDecodeError:
        return {"raw_response": result}