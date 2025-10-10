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
         "tipo_punto" : "puntos_medicion",
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
         "fecha_modificacion" : "1758818476306",
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
ArcGIS-Data/Puntos/{ID}_{tipo_punto}/Capa_principal/{latest-timestamp}.json
'''


import boto3
import json
from pathlib import Path
from docxtpl import DocxTemplate, InlineImage
from datetime import datetime
from docx.shared import Cm
import os


client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

s3 = boto3.client("s3")
TMP_DIR = Path("/tmp")

bucket_name = os.environ['BUCKET_NAME']
template_path_s3 = "files/plantillas/Fase1/"
output_path_s3 = "files/entregables/Fase1/"

template_name = {"circuito": "informe-acueducto.docx",
                 "cuenca": "informe-alcantarillado.docx"}

COD_name = {"circuito": "ACU/MPH-EJ-06-01-F01-ACU-DIA-", "cuenca": "ALC/MPH-EJ-06-01-F01-ALC-DIA-"}


def lambda_handler(event, context):   
    payload_data = event["payload"]["attributes"]
    tipo_punto = event["payload"]["attributes"]["tipo_punto"]
    id = event["payload"]["attributes"]["id"]

    capa_principal_data = obtener_info_de_capa_principal(bucket_name, payload_data)

    if tipo_punto == "vrp" or tipo_punto == "puntos_medicion":
        circuito_cuenca_valor = capa_principal_data.get("circuito", "N/A")
        circuito_cuenca = "circuito"
    else:
        circuito_cuenca_valor = capa_principal_data.get("cuenca", "N/A")
        circuito_cuenca = "cuenca"

    print(circuito_cuenca, circuito_cuenca_valor)

    template_key = template_path_s3 + template_name.get(circuito_cuenca)

    # Paths locales en Lambda (/tmp)
    template_path = TMP_DIR / "plantilla.docx"
    output_path = TMP_DIR / "output.docx"

    # Descargar archivos desde S3
    s3.download_file(bucket_name, template_key, str(template_path))

    contexto = build_general_context(circuito_cuenca_valor, circuito_cuenca)

    doc = DocxTemplate(template_path)
    doc.render(contexto)
    doc.save(output_path)

    # Subir resultado a S3
    output_key = f"{output_path_s3}{COD_name[circuito_cuenca]}{circuito_cuenca_valor}.docx"
    s3.upload_file(str(output_path), bucket_name, output_key)

    return {
        "status": "ok",
        "output_file": f"s3://{bucket_name}/{output_key}"
    }

def obtener_info_de_capa_principal(bucket_name, payload_data):
    # Construir el prefijo correcto
    s3_key_capa_principal = (
        f"ArcGIS-Data/Puntos/{payload_data['id']}_{payload_data['tipo_punto']}/Capa_principal/"
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
    SELECT
        -- Totales por tipo desde puntos_capa_principal
        (SELECT COUNT(*) 
         FROM puntos_capa_principal 
         WHERE circuito = '{circuito}' 
           AND tipo_punto = 'puntos_medicion') AS numero_puntos_medicion_totales,

        (SELECT COUNT(*) 
         FROM puntos_capa_principal 
         WHERE circuito = '{circuito}' 
           AND tipo_punto = 'vrp') AS numero_vrp_totales,

        -- Visitas en fase_1
        COUNT(DISTINCT CASE WHEN tipo_punto = 'puntos_medicion' THEN id END) AS numero_puntos_medicion_visitadas,
        COUNT(DISTINCT CASE WHEN tipo_punto = 'vrp' THEN id END) AS numero_vrp_visitadas,

        -- Fechas mínima y máxima
        MIN(fecha_creacion) AS fecha_primera_visita,
        MAX(fecha_creacion) AS fecha_ultima_visita,

        -- Vulnerables
        COUNT(*) FILTER (WHERE tipo_punto = 'puntos_medicion' AND vulnerabilidad = 1) AS puntos_vulnerables,
        COUNT(*) FILTER (WHERE tipo_punto = 'vrp' AND vulnerabilidad = 1) AS vrp_vulnerables,

        -- Clausura
        COUNT(*) FILTER (WHERE tipo_punto = 'puntos_medicion' AND requiere_clausura = 1) AS puntos_clausurar,
        COUNT(*) FILTER (WHERE tipo_punto = 'vrp' AND requiere_clausura = 1) AS vrp_clausurar,

        -- Condición física OK
        COUNT(*) FILTER (WHERE tipo_punto = 'puntos_medicion' AND condicion_fisica_general = 0) AS puntos_cond_ok,
        COUNT(*) FILTER (WHERE tipo_punto = 'vrp' AND condicion_fisica_general = 0) AS vrp_cond_ok,

        -- Conexiones hidráulicas OK
        COUNT(*) FILTER (WHERE tipo_punto = 'puntos_medicion' AND conexiones_hidraulicas = 0) AS puntos_hid_ok,
        COUNT(*) FILTER (WHERE tipo_punto = 'vrp' AND conexiones_hidraulicas = 0) AS vrp_hid_ok,

        -- Habilitado medición OK
        COUNT(*) FILTER (WHERE tipo_punto = 'puntos_medicion' AND habilitado_medicion = 1) AS puntos_ok,
        COUNT(*) FILTER (WHERE tipo_punto = 'vrp' AND habilitado_medicion = 1) AS vrp_ok,

        -- Instalación tapa
        COUNT(*) FILTER (WHERE tipo_punto = 'puntos_medicion' AND requiere_instalacion_tapa = 1) AS puntos_tapa

    FROM fase_1
    WHERE circuito = '{circuito}';
    """

    # --- UNA SOLA LLAMADA A LA LAMBDA ---
    result = query_db(query)[0]
    print(result)

    # Procesamiento de fechas
    fecha_primera_visita_ts = result["fecha_primera_visita"]
    fecha_ultima_visita_ts = result["fecha_ultima_visita"]

    fecha_primera_visita = (
        datetime.fromtimestamp(fecha_primera_visita_ts / 1000).strftime('%d/%m/%Y')
        if fecha_primera_visita_ts else "N/A"
    )

    fecha_ultima_visita = (
        datetime.fromtimestamp(fecha_ultima_visita_ts / 1000).strftime('%d/%m/%Y')
        if fecha_ultima_visita_ts else "N/A"
    )

    # Cálculo de días totales
    if fecha_primera_visita_ts and fecha_ultima_visita_ts:
        total_dias = (fecha_ultima_visita_ts - fecha_primera_visita_ts) // (1000 * 60 * 60 * 24)
    else:
        total_dias = 0

    # Otros cálculos derivados
    numero_puntos_medicion_visitadas = result["numero_puntos_medicion_visitadas"]
    numero_vrp_visitadas = result["numero_vrp_visitadas"]

    numero_total_visitas = numero_puntos_medicion_visitadas + numero_vrp_visitadas
    puntos_intervencion = numero_puntos_medicion_visitadas - result["puntos_ok"]
    vrp_intervencion = numero_vrp_visitadas - result["vrp_ok"]

    return {
        "nombre_circuito": circuito,
        "numero_puntos_medicion_totales": result["numero_puntos_medicion_totales"],
        "numero_vrp_totales": result["numero_vrp_totales"],
        "numero_puntos_medicion_visitadas": numero_puntos_medicion_visitadas,
        "numero_vrp_visitadas": numero_vrp_visitadas,
        "fecha_primera_visita": fecha_primera_visita,
        "fecha_ultima_visita": fecha_ultima_visita,
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


def query_db(query):
    payload_db = {
        "queryStringParameters": {
            "query": query,
            "time_column": "fecha_creacion",
            "db_name": "parametros"
        }
    }
    response_db = invoke_lambda_db(payload_db, db_access_arn)
    body = json.loads(response_db["body"])
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