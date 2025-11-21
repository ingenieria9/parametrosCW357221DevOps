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
         "REQUIERE_INST_TAPA_ACU" : "Si",
         "REQ_LIMPIEZA_ACU" : "No",
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
from botocore.exceptions import ClientError
import json
from pathlib import Path
from docxtpl import DocxTemplate, InlineImage
from datetime import datetime, timezone, timedelta
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

COD_name = {"circuito": "ACU/CIR/MPH-EJ-0601-{COD}-F01-ACU-DIA-001",
            "cuenca": "ALC/CUE/MPH-EJ-0601-{COD}-F01-ALC-DIA-001"}


def lambda_handler(event, context):   
    payload_data = event["payload"]
    TIPO_PUNTO = event["payload"]["TIPO_PUNTO"]
    FID_ELEM = event["payload"]["FID_ELEM"]
    GlobalID = event["payload"]["PARENT_ID"]
    CIRCUITO_ACU = event["payload"]["CIRCUITO_ACU"].replace(" ", "_")

    # --- SISTEMA DE LOCK POR CIRCUITO ---
    lock_key = f"locks/{CIRCUITO_ACU}.lock"

    # Verificar si ya existe un lock para este circuito
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=lock_key,
            Body=b"LOCK",
            ContentType='text/plain',
            Metadata={'created': datetime.utcnow().isoformat()},
            IfNoneMatch='*'
        )
        print(f"Lock creado para {CIRCUITO_ACU}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'PreconditionFailed':
            print(f"Otro proceso ya tiene lock para {CIRCUITO_ACU}")
            return {
                "statusCode": 200,
                "body": f"Lambda saltada: lock activo para {CIRCUITO_ACU}"
            }
        raise

    try:
        capa_principal_data = obtener_info_de_capa_principal(bucket_name, TIPO_PUNTO, GlobalID, CIRCUITO_ACU)

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

        '''
        try:
            s3.head_object(Bucket=bucket_name, Key=output_key)
            print(f"El archivo {output_key} ya existe en S3. Saliendo de la función.")
            return {
                "statusCode": 200,
                "body": f"Archivo ya existente: s3://{bucket_name}/{output_key}"
            }
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"El archivo {output_key} no existe. Continuando con la generación...")
            else:
                # Otro error (no de tipo "no encontrado")
                raise
        '''



        contexto_general = build_general_context(circuito_cuenca_valor, circuito_cuenca, capa_principal_data)

        #print("contexto_general", contexto_general)

        contexto_puntos_ok = build_puntos_context(circuito_cuenca_valor, circuito_cuenca, 1, code_data)
        contexto_puntos_not_ok = build_puntos_context(circuito_cuenca_valor, circuito_cuenca, 0, code_data)
        contexto_puntos_no_encontrados = build_puntos_context(circuito_cuenca_valor, circuito_cuenca, 2, code_data)

        datos_base = build_datos_base(circuito_cuenca_valor, circuito_cuenca,  code_data)
        datos_resumen = build_datos_resumen(circuito_cuenca_valor, circuito_cuenca, code_data)

        print(contexto_puntos_not_ok)
        print(contexto_puntos_ok)

        doc = DocxTemplate(template_path)

        imagenes = obtener_imagenes_grafana(doc, circuito_cuenca_valor, circuito_cuenca)

        print(imagenes)
        
        contexto = {**contexto_general,  "puntos_fase3": contexto_puntos_ok, "puntos_fase2": contexto_puntos_not_ok, "puntos_no_encontrados_lista" : contexto_puntos_no_encontrados, "datos_base" : datos_base, "datos_resumen":datos_resumen,  **imagenes}
        print(contexto)
        contexto = replace_none_with_empty(contexto)
        
        doc.render(contexto)
        

        for table in doc.tables:
            for row in table.rows:
                # Si todas las celdas están vacías o con espacios
                if all(not cell.text.strip() for cell in row.cells):
                    row._element.getparent().remove(row._element)

        doc.save(output_path)


        

        
        s3.upload_file(str(output_path), bucket_name, output_key)
        
        return {
            "status": "ok",
            "output_file": f"s3://{bucket_name}/{output_key}"
        }
    
    finally:
        # --- Liberar el lock ---
        try:
            s3.delete_object(Bucket=bucket_name, Key=lock_key)
            print(f"✅ Lock liberado para {CIRCUITO_ACU}")
        except Exception as e:
            print(f"⚠️ Error al eliminar lock para {CIRCUITO_ACU}: {e}")

def replace_none_with_empty(obj):
    """Recorre dicts/listas y reemplaza None por '' (string vacío)."""
    if isinstance(obj, dict):
        return {k: replace_none_with_empty(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_none_with_empty(item) for item in obj]
    elif obj is None:
        return ""
    else:
        return obj


def obtener_imagenes_grafana(doc, circuito_cuenca_valor, circuito_cuenca):
    """
    Retorna un diccionario con las imágenes renderizadas desde Grafana listas
    para usarse en docxtpl.render(contexto).
    Las claves del diccionario son del tipo 'grafico_panel_{id}'.
    """
    GRAFANA_URL = "https://iot-grupoepm.teleprocess.co"
    token = os.environ["GRAFANA_API_ACCESS"]

    # Define los paneles según el tipo
    if circuito_cuenca == 'circuito':
        panel_ids = [8, 9, 10]
    elif circuito_cuenca == 'cuenca':
        panel_ids = [6, 7, 8]  # puedes ajustar estos IDs
    else:
        panel_ids = []

    imagenes = {}

    for panel_id in panel_ids:
        render_url = (
            f"{GRAFANA_URL}/render/d-solo/3b0bc7f5-c3ff-4728-a365-ebc795591190/"
            f"graficos-informe?orgId=9&panelId={panel_id}&var-{circuito_cuenca}={circuito_cuenca_valor}"
        )

        headers = {"Authorization": f"Bearer {token}"}

        print(f"Descargando panel {panel_id}...")
        response = requests.get(render_url, headers=headers)
        response.raise_for_status()

        img_path = f"/tmp/grafico_panel_{panel_id}.png"
        with open(img_path, "wb") as f:
            f.write(response.content)

        imagenes[f"grafico_panel_{panel_id}"] = InlineImage(
            doc, img_path, width=Cm(10), height=Cm(5)
        )

        print(f"✔ Panel {panel_id} descargado ({img_path})")

    return imagenes
    

def obtener_info_de_capa_principal(bucket_name, TIPO_PUNTO, GlobalID, CIRCUITO_ACU):
    # Construir el prefijo correcto
    s3_key_capa_principal = (
        f"ArcGIS-Data/Puntos/{CIRCUITO_ACU}/{GlobalID}_{TIPO_PUNTO}/Capa_principal/"
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


def build_datos_base(circuito_cuenca_valor, circuito_cuenca, code_data):
    # Determinar el campo correcto según el tipo
    if circuito_cuenca == "circuito":
        circuito_cuenca = "CIRCUITO_ACU"
    else: 
        circuito_cuenca = "CUENCA_ALC"

    # Armar el query según habilitado_medicion (aunque aquí no lo usas en el WHERE)
   
    query = f"""
    SELECT "FID_ELEM", "TIPO_PUNTO", "DIRECCION_ACU", "SUBCIRCUIT_ACU",  
            "FASE_INICIAL", "PUNTO_EXISTENTE", "VARIABLE_A_MEDIR"
    FROM puntos_capa_principal pcp
    WHERE "{circuito_cuenca}" = '{circuito_cuenca_valor}'
    """

    print(query)

    # Ejecutar el query
    resultados = query_db(query, "fecha_creacion")
    print(resultados)
    # resultados debería ser una lista de dicts, ejemplo:
    # [{'FID_ELEM': 'P001', 'TIPO_PUNTO': 'camara', ...}, {...}, ...]

    # Construir la lista que irá al contexto de docxtpl
    puntos_visitados_consolidados = []

    for row in resultados:
        punto_filtrado = {
            "FID_ELEM": row.get("FID_ELEM"),
            "TIPO_PUNTO": row.get("TIPO_PUNTO"),
            "DIRECCION_ACU": row.get("DIRECCION_ACU"),
            "SUBCIRCUIT_ACU": row.get("SUBCIRCUIT_ACU"),
            "FASE_INICIAL": row.get("FASE_INICIAL"),
            "PUNTO_EXISTENTE": row.get("PUNTO_EXISTENTE"),
            "VARIABLE_A_MEDIR": row.get("VARIABLE_A_MEDIR")
        }

        puntos_visitados_consolidados.append(punto_filtrado)

    return puntos_visitados_consolidados


def build_datos_resumen(circuito_cuenca_valor, circuito_cuenca, code_data):
    # Determinar el campo correcto según el tipo
    if circuito_cuenca == "circuito":
        circuito_cuenca = "CIRCUITO_ACU"
    else: 
        circuito_cuenca = "CUENCA_ALC"

    # Armar el query según habilitado_medicion (aunque aquí no lo usas en el WHERE)
   
    query = f"""
    SELECT 
    pcp."FID_ELEM" as "FID_ELEM"  ,
    pcp."TIPO_PUNTO" as "TIPO_PUNTO", 
    f."EXPOSICION_FRAUDE" AS "EXPOSICION_FRAUDE", 
    f."PUNTO_ENCONTRADO" AS "PUNTO_ENCONTRADO" ,
    CASE 
        WHEN f."REQ_LIMPIEZA_ACU" = 1 THEN 'Si'
        WHEN f."REQ_LIMPIEZA_ACU" = 0 THEN 'No'
        ELSE NULL
    END AS "REQ_LIMPIEZA_ACU",
    CASE 
        WHEN f."REQUIERE_INST_TAPA_ACU" = 1 THEN 'Si'
        WHEN f."REQUIERE_INST_TAPA_ACU" = 0 THEN 'No'
        ELSE NULL
    END AS "REQUIERE_INST_TAPA_ACU",
    CASE 
        WHEN f."conexiones_hidraulicas" = 1 THEN 'buena'
        WHEN f."conexiones_hidraulicas" = 0 THEN 'mala'
        ELSE NULL
    END AS conexiones_hidraulicas,
    CASE 
        WHEN f."condicion_fisica_general" = 1 THEN 'buena'
        WHEN f."condicion_fisica_general" = 0 THEN 'mala'
        ELSE NULL
    END AS condicion_fisica_general
    FROM fase_1 f
    INNER JOIN puntos_capa_principal pcp 
    ON f."PARENT_ID" = pcp."GlobalID"
    WHERE pcp."{circuito_cuenca}" = '{circuito_cuenca_valor}';
    """

    print(query)

    # Ejecutar el query
    resultados = query_db(query, "fecha_creacion")
    print(resultados)
    # resultados debería ser una lista de dicts, ejemplo:
    # [{'FID_ELEM': 'P001', 'TIPO_PUNTO': 'camara', ...}, {...}, ...]

    # Construir la lista que irá al contexto de docxtpl
    puntos_visitados_consolidados = []

    for row in resultados:
        punto_filtrado = {
            "FID_ELEM": row.get("FID_ELEM"),
            "TIPO_PUNTO": row.get("TIPO_PUNTO"),
            "EXPOSICION_FRAUDE": row.get("EXPOSICION_FRAUDE"),
            "PUNTO_ENCONTRADO": row.get("PUNTO_ENCONTRADO"),
            "REQ_LIMPIEZA_ACU": row.get("REQ_LIMPIEZA_ACU"),
            "REQUIERE_INST_TAPA_ACU": row.get("REQUIERE_INST_TAPA_ACU"),
            "conexiones_hidraulicas": row.get("conexiones_hidraulicas"),
            "condicion_fisica_general": row.get("condicion_fisica_general"),
            "REQ_LIMPIEZA_ACU" :  row.get("REQ_LIMPIEZA_ACU"),
            "REQUIERE_INST_TAPA_ACU": row.get("REQUIERE_INST_TAPA_ACU"),
        }

        puntos_visitados_consolidados.append(punto_filtrado)

    return puntos_visitados_consolidados



def build_puntos_context(circuito_cuenca_valor, circuito_cuenca, habilitado_medicion, code_data):
    if circuito_cuenca == "circuito":
        circuito_cuenca = "CIRCUITO_ACU"
    else: 
        circuito_cuenca = "CUENCA_ALC"

    # 1. Obtener lista de id y TIPO_PUNTO

    if habilitado_medicion == 1:
        query = f"""
        SELECT p."GlobalID", p."TIPO_PUNTO" 
        FROM puntos_capa_principal  p
        INNER JOIN fase_1 f ON f."PARENT_ID"  = p."GlobalID"
        WHERE p."{circuito_cuenca}" = '{circuito_cuenca_valor}' and f."habilitado_medicion" = 1
        """
    elif habilitado_medicion == 0: 
        query = f"""
        SELECT p."GlobalID", p."TIPO_PUNTO" 
        FROM puntos_capa_principal  p
        INNER JOIN fase_1 f ON f."PARENT_ID"  = p."GlobalID"
        WHERE p."{circuito_cuenca}" = '{circuito_cuenca_valor}' and f."habilitado_medicion" = 0 and f."PUNTO_ENCONTRADO" = 'Si'
        """
    else:
        query = f"""
        SELECT p."GlobalID", p."TIPO_PUNTO" 
        FROM puntos_capa_principal  p
        INNER JOIN fase_1 f ON f."PARENT_ID"  = p."GlobalID"
        WHERE p."{circuito_cuenca}" = '{circuito_cuenca_valor}' and "PUNTO_ENCONTRADO" = 'No'
        """

    print(query)
    resultados = query_db(query, "fecha_creacion")

    # Extraer listas de id y TIPO_PUNTO
    lista_id = [row["GlobalID"] for row in resultados]
    lista_tipo = [row["TIPO_PUNTO"] for row in resultados]

    print(lista_id)
    print(lista_tipo)


    # Diccionario con los formatos por tipo de punto - misma que se usa en lambda formato fase 1
    COD_name = {
        "puntos_medicion": "MPH-EJ-0601-{COD}-F01-ACU-EIN-001",
        "vrp": "MPH-EJ-0601-{COD}-F01-ACU-EIN-001",
        "camara": "MPH-EJ-0601-{COD}-F01-ALC-EIN-001"
    }

    puntos_visitados_consolidados = []

    for punto, TIPO_PUNTO in zip(lista_id, lista_tipo):
        key_s3_prefix = f"ArcGIS-Data/Puntos/{circuito_cuenca_valor.replace(' ', '_')}/{punto}_{TIPO_PUNTO}/Fase1/"
        print(key_s3_prefix)
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
            "PUNTOS_HABILITADO_FASE3",
            "EXPOSICION_FRAUDE",
            "UBICACION_GEO_CRITICA"
        ]

        punto_filtrado = {
            "TIPO_PUNTO": TIPO_PUNTO,
            "archivo_s3": latest_key
        }

        # Validamos que 'attributes' exista (info esta en attributes)
        #data_attr = json_data.get("attributes", {})
        data_attr = json_data
        print(data_attr)      

        # Construir nombre_formato
        FID_ELEM = data_attr.get("FID_ELEM", "SIN_FID")
        base_name = COD_name.get(TIPO_PUNTO, "").format(COD=code_data)
        nombre_formato = f"{base_name}.xlsx"
        punto_filtrado["nombre_formato"] = nombre_formato

        for attr in atributos_deseados:
            punto_filtrado[attr] = data_attr.get(attr)

        puntos_visitados_consolidados.append(punto_filtrado)

    return puntos_visitados_consolidados

def build_general_context(circuito_cuenca_valor, circuito_cuenca, capa_principal_data):

    if circuito_cuenca == "circuito":
        data = get_general_data_circuito(circuito_cuenca_valor, capa_principal_data)
    else:   
        data = {}  # Implementar función similar para cuenca 

    print("data", data)
    
    campos_contexto = list(data.keys())

    # Construir contexto final
    context = {
        campo: data.get(campo, "")
        for campo in data.keys()
    }

    return context

def get_general_data_circuito(circuito, capa_principal_data):
    query = f"""
        WITH puntos_filtrados AS (
            SELECT "GlobalID", "TIPO_PUNTO"
            FROM puntos_capa_principal
            WHERE "CIRCUITO_ACU" = '{circuito}'
        ),
        fase_datos AS (
            SELECT 
                f."FID_ELEM",
                f."REQUIERE_FASE1",
                f."habilitado_medicion",
                f."UBICACION_GEO_CRITICA",
                f."condicion_fisica_general",
                f."conexiones_hidraulicas",
                f."REQ_LIMPIEZA_ACU",
                f."REQUIERE_INST_TAPA_ACU",
                f."PUNTO_ENCONTRADO",
                p."TIPO_PUNTO",
                p."GlobalID"
            FROM fase_1 f
            INNER JOIN puntos_filtrados p ON f."PARENT_ID" = p."GlobalID"
        )
        SELECT
            -- Totales por tipo desde puntos_capa_principal
            (SELECT COUNT(*) 
            FROM puntos_capa_principal 
            WHERE "CIRCUITO_ACU" = '{circuito}'
            AND "TIPO_PUNTO" = 'puntos_medicion'
            AND "PUNTO_EXISTENTE" = 'Si') AS puntos_medicion_existentes,

            (SELECT COUNT(*) 
            FROM puntos_capa_principal 
            WHERE "CIRCUITO_ACU" = '{circuito}'
            AND "TIPO_PUNTO" = 'puntos_medicion'
            AND "PUNTO_EXISTENTE" = 'No') AS puntos_medicion_proyectados,

            (SELECT COUNT(*) 
            FROM puntos_capa_principal 
            WHERE "CIRCUITO_ACU" = '{circuito}'
            AND "TIPO_PUNTO" = 'vrp') AS vrp_circuito,

            (SELECT COUNT(*) 
            FROM puntos_capa_principal 
            WHERE "CIRCUITO_ACU" = '{circuito}'
            AND "TIPO_PUNTO" = 'puntos_medicion'
            AND "PUNTO_EXISTENTE" = 'Si'
            AND "FASE_INICIAL" != 'fase3') AS puntos_medicion_fase1,

            -- Visitas en fase_1 (JOIN con ids del circuito)
            COUNT(DISTINCT CASE WHEN f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' THEN f."FID_ELEM" END) AS puntos_medicion_visitados,
            COUNT(DISTINCT CASE WHEN f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' THEN f."FID_ELEM" END) AS vrp_visitadas,

            -- Habilitados Fase 3
            COUNT(DISTINCT CASE WHEN f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."habilitado_medicion" = 1 THEN f."FID_ELEM" END) AS puntos_medicion_habilitados_fase3,
            COUNT(DISTINCT CASE WHEN f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."habilitado_medicion" = 1 THEN f."FID_ELEM" END) AS vrp_habilitados_fase3,
            COUNT(DISTINCT CASE WHEN f."REQUIERE_FASE1" = 'Si' AND f."habilitado_medicion" = 1 THEN f."FID_ELEM" END) AS total_habilitados_fase3,

            -- No habilitados fase 3
            COUNT(DISTINCT CASE WHEN f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND (f."habilitado_medicion" = 0 OR f."habilitado_medicion" IS NULL) THEN f."FID_ELEM" END) AS puntos_medicion_no_habilitados_fase3,
            COUNT(DISTINCT CASE WHEN f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND (f."habilitado_medicion" = 0 OR f."habilitado_medicion" IS NULL) THEN f."FID_ELEM" END) AS vrp_no_habilitados_fase3,
            COUNT(DISTINCT CASE WHEN f."REQUIERE_FASE1" = 'Si' AND (f."habilitado_medicion" = 0 OR f."habilitado_medicion" IS NULL) THEN f."FID_ELEM" END) AS total_no_habilitados_fase3,

            -- Ubicación crítica
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."UBICACION_GEO_CRITICA" > 3) AS puntos_ubi_criticos,
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."UBICACION_GEO_CRITICA" > 3) AS vrp_ubi_critica,

            -- Ajustes condición física
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."condicion_fisica_general" = 0) AS puntos_condicion_fisica,
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."condicion_fisica_general" = 0) AS vrp_condicion_fisica,

            -- Ajustes hidráulicos
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."conexiones_hidraulicas" = 0) AS puntos_ajuste_hidraulico,
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."conexiones_hidraulicas" = 0) AS vrp_ajuste_hidraulico,

            -- Requiere limpieza
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."REQ_LIMPIEZA_ACU" = 1) AS puntos_limpieza,
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."REQ_LIMPIEZA_ACU" = 1) AS vrp_limpieza,

            -- Instalación tapa
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."REQUIERE_INST_TAPA_ACU" = 1) AS puntos_tapa,
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."REQUIERE_INST_TAPA_ACU" = 1) AS vrp_tapa,

            -- Puntos no encontrados
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'puntos_medicion' AND f."REQUIERE_FASE1" = 'Si' AND f."PUNTO_ENCONTRADO" = 'No') AS puntos_no_encontrados,
            COUNT(*) FILTER (WHERE f."TIPO_PUNTO" = 'vrp' AND f."REQUIERE_FASE1" = 'Si' AND f."PUNTO_ENCONTRADO" = 'No') AS vrp_no_encontrados,

            -- NUEVOS CAMPOS (de los SELECT adicionales)
            (SELECT COUNT(p."GlobalID")
            FROM puntos_capa_principal p
            INNER JOIN fase_1 f ON f."PARENT_ID" = p."GlobalID"
            WHERE p."CIRCUITO_ACU" = '{circuito}'
            AND f."habilitado_medicion" = 1
            AND p."PUNTO_EXISTENTE" = 'Si') AS numero_puntos_habilitados_fase3,

            (SELECT COUNT(p."GlobalID")
            FROM puntos_capa_principal p
            WHERE p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'puntos_medicion'
            AND p."PUNTO_EXISTENTE" = 'No') AS puntos_proyectados,

            (SELECT COUNT(p."GlobalID")
            FROM puntos_capa_principal p
            WHERE p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'vrp'
            AND p."FASE_INICIAL" = 'fase3'
            AND p."PUNTO_EXISTENTE" = 'Si') AS vrp_fase3,

            (SELECT COUNT(p."GlobalID")
            FROM puntos_capa_principal p
            INNER JOIN fase_1 f ON f."PARENT_ID" = p."GlobalID"
            WHERE p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'puntos_medicion'
            AND (f."habilitado_medicion" = 0 OR f."habilitado_medicion" IS NULL)
            AND p."PUNTO_EXISTENTE" = 'Si') AS puntos_intervencion,

            (SELECT COUNT(p."GlobalID")
            FROM puntos_capa_principal p
            INNER JOIN fase_1 f ON f."PARENT_ID" = p."GlobalID"
            WHERE p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'vrp'
            AND f."habilitado_medicion" = 1
            AND p."PUNTO_EXISTENTE" = 'Si') AS vrp_habilitados_fase3,

            -- Porcentaje habilitados
            ROUND(
            100.0 * (
                (SELECT COUNT(p."GlobalID")::numeric
                FROM puntos_capa_principal p
                INNER JOIN fase_1 f ON f."PARENT_ID" = p."GlobalID"
                WHERE p."CIRCUITO_ACU" = '{circuito}'
                AND f."habilitado_medicion" = 1
                AND p."PUNTO_EXISTENTE" = 'Si')
                /
                NULLIF(
                (SELECT COUNT(*) FROM puntos_capa_principal 
                WHERE "CIRCUITO_ACU" = '{circuito}'
                    AND "PUNTO_EXISTENTE" = 'Si' and "FASE_INICIAL" != 'fase3')::numeric,
                0::numeric)
            ), 2
            ) AS porcentaje_puntos_habilitados_fase3
        FROM fase_datos f;"""

    # --- UNA SOLA LLAMADA A LA LAMBDA ---
    result = query_db(query, "FECHA_CREACION")[0]
    print("result", result)

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
    numero_puntos_medicion_visitadas = result["puntos_medicion_visitados"]
    numero_vrp_visitadas = result["vrp_visitadas"]

    numero_total_visitas = numero_puntos_medicion_visitadas + numero_vrp_visitadas

    fecha_primera_visita_date = formatear_fecha(fecha_primera_visita)
    fecha_ultima_visita_date = formatear_fecha(fecha_ultima_visita)

    #Fecha reporte
    utc_minus_5 = timezone(timedelta(hours=-5))
    fecha_actual = datetime.now(utc_minus_5)
    fecha_reporte = fecha_actual.strftime("%Y-%m-%d %H:%M:%S")

    #otro query 
    query_2 = f"""WITH datos AS (
        -- Puntos de medición habilitados Fase 3
        SELECT 
            COUNT(p."GlobalID") AS puntos_habilitados_f3,
            0 AS puntos_proyectados,
            0 AS vrp_f3,
            0 AS puntos_intervencion
        FROM puntos_capa_principal p
        INNER JOIN fase_1 f ON f."PARENT_ID" = p."GlobalID"
        WHERE 
            p."CIRCUITO_ACU" = '{circuito}'
            AND p."PUNTO_EXISTENTE" = 'Si'
            AND f."habilitado_medicion" = 1

        UNION ALL

        -- Puntos proyectados (no existentes)
        SELECT 
            0,
            COUNT(p."GlobalID"),
            0,
            0
        FROM puntos_capa_principal p
        WHERE 
            p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'puntos_medicion'
            AND p."PUNTO_EXISTENTE" = 'No'

        UNION ALL

        -- VRP directas para Fase 3
        SELECT 
            0,
            0,
            COUNT(p."GlobalID"),
            0
        FROM puntos_capa_principal p
        WHERE 
            p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'vrp'
            AND p."FASE_INICIAL" = 'fase3'
            AND p."PUNTO_EXISTENTE" = 'Si'

        UNION ALL

        -- Puntos para intervención
        SELECT 
            0,
            0,
            0,
            COUNT(p."GlobalID")
        FROM puntos_capa_principal p
        INNER JOIN fase_1 f ON f."PARENT_ID" = p."GlobalID"
        WHERE 
            p."CIRCUITO_ACU" = '{circuito}'
            AND p."TIPO_PUNTO" = 'puntos_medicion'
            AND p."PUNTO_EXISTENTE" = 'Si'
            AND (f."habilitado_medicion" = 0 OR f."habilitado_medicion" IS NULL)
        )

        SELECT
        (SUM(puntos_habilitados_f3) + SUM(vrp_f3)) AS "numero_fase_3_final",
        (SUM(puntos_proyectados) + SUM(puntos_intervencion)) AS "numero_fase_2_final",
        (SUM(puntos_habilitados_f3) + SUM(vrp_f3) + SUM(puntos_proyectados) + SUM(puntos_intervencion)) AS "Total Puntos",
        ROUND(
            100.0 * (SUM(puntos_habilitados_f3) + SUM(vrp_f3)) /
            NULLIF(SUM(puntos_habilitados_f3) + SUM(vrp_f3) + SUM(puntos_proyectados) + SUM(puntos_intervencion), 0),
            2
        ) AS "porcentaje_fase3_final",
        ROUND(
            100.0 * (SUM(puntos_proyectados) + SUM(puntos_intervencion)) /
            NULLIF(SUM(puntos_habilitados_f3) + SUM(vrp_f3) + SUM(puntos_proyectados) + SUM(puntos_intervencion), 0),
            2
        ) AS "porcentaje_fase_2_final"
        FROM datos;"""

    # --- OTRA LLAMADA A LA LAMBDA ---
    result_2 = query_db(query_2, "FECHA_CREACION")[0]
    print("result2", result_2)
            
    return {
        "nombre_circuito": circuito,
        "puntos_medicion_existentes": result["puntos_medicion_existentes"],
        "puntos_medicion_proyectados": result["puntos_medicion_proyectados"],
        "vrp_circuito": result["vrp_circuito"],
        "puntos_medicion_visitados": result["puntos_medicion_visitados"],
        "vrp_visitadas": result["vrp_visitadas"],
        "puntos_medicion_habilitados_fase3" : result["puntos_medicion_habilitados_fase3"],
        "vrp_habilitados_fase3" : result["vrp_habilitados_fase3"],
        "fecha_primera_visita": fecha_primera_visita_date,
        "fecha_ultima_visita": fecha_ultima_visita_date,
        "total_dias": total_dias,
        "numero_total_visitas": numero_total_visitas,
        "puntos_ubi_criticos": result["puntos_ubi_criticos"],
        "vrp_ubi_critica": result["vrp_ubi_critica"],
        "puntos_tapa": result["puntos_tapa"],
        "vrp_tapa" : result["vrp_tapa"],
        "porcentaje_puntos_habilitados_fase3": result["porcentaje_puntos_habilitados_fase3"],
        "fecha_reporte" : fecha_reporte,
        "municipio" : capa_principal_data.get("MUNICIPIO_ACU", "N/A"),
        "puntos_no_encontrados" : result["puntos_no_encontrados"],
        "vrp_no_encontrados" : result["vrp_no_encontrados"],
        "puntos_medicion_fase1" : result["puntos_medicion_fase1"],
        "total_habilitados_fase3" : result["total_habilitados_fase3"],
        "total_no_habilitados_fase3" : result["total_no_habilitados_fase3"],
        "puntos_ajuste_hidraulico" : result["puntos_ajuste_hidraulico"],
        "vrp_ajuste_hidraulico" : result["vrp_ajuste_hidraulico"],
        "puntos_condicion_fisica" : result["puntos_condicion_fisica"],
        "vrp_condicion_fisica" :  result["vrp_condicion_fisica"],
        "vrp_limpieza" : result["vrp_limpieza"],
        "puntos_limpieza" : result["puntos_limpieza"],
        "numero_puntos_habilitados_fase3" : result["numero_puntos_habilitados_fase3"],
        "vrp_fase3" : result["vrp_fase3"],
        "puntos_totales_fase_3" : result["numero_puntos_habilitados_fase3"] + result["vrp_fase3"],
        "porcentaje_fase3_final": result_2["porcentaje_fase3_final"],
        "numero_fase_3_final" : result_2["numero_fase_3_final"],
        "porcentaje_fase_2_final" : result_2["porcentaje_fase_2_final"],
        "numero_fase_2_final" : result_2["numero_fase_2_final"],
        "puntos_proyectados" : result["puntos_proyectados"],
        "puntos_intervencion" : result["puntos_intervencion"]
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