#Ejemplo de payload 
#{'payload': {'layer_id': 1, 'OBJECTID': '0002', 'geometry': 'null', 'attributes': {'OBJECTID': '0002', 'GlobalID': '3CFDE950-8AE7-440E-B1E7-310C56A35794', 'Identificador': 'PTO_0002', 'Tipo_Punto': 'VRP', 'Creador': 'central_ti_telemetrik', 'Fecha_Creacion': 1758818476306, 'Editor': 'central_ti_telemetrik', 'Fecha_Edicion': 1758829344252, 'Sí': 'Sí', 'Fugas': 'No', 'Signos_de_desgaste': 'null'}, 'point_type': 'VRP'}, 'attachments': ['CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg']}

#incoming_payload = event
#incoming_payload = {'payload': {'layer_id': 1, 'OBJECTID': '0002', 'geometry': 'null', 'attributes': {'OBJECTID': '0002', 'GlobalID': '3CFDE950-8AE7-440E-B1E7-310C56A35794', 'Identificador': 'PTO_0002', 'Tipo_Punto': 'VRP', 'Creador': 'central_ti_telemetrik', 'Fecha_Creacion': 1758818476306, 'Editor': 'central_ti_telemetrik', 'Fecha_Edicion': 1758829344252, 'Sí': 'Sí', 'Fugas': 'No', 'Signos_de_desgaste': 'null'}, 'point_type': 'VRP'}, 'attachments': ['CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg']}

#ejemplo de payload 
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
         "circuito" : "tmk",
         "tipo_punto" : "caja_medicion",
         "vrp" : "vrp-0001",
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
         "direccion_referencia" : "Cra 42 #2 cerca al mall",
         "actualizacion_ubicacion" : "No",
         "fecha_creacion" : "1758818476306",
         "latitud" : "37.21",
         "longitud" : "-72.912"
      },
      "point_type":"cajas_medicion"
   },
   "attachments":[
      "files/temp-image-folder/ejemplo4.jpg",
      "files/temp-image-folder/ejemplo1.jpg",
      "files/temp-image-folder/ejemplo2.jpg"
   ]
} '''

import boto3
import json
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.drawing.image import Image  # para insertar imágenes
import os

s3 = boto3.client("s3")
TMP_DIR = Path("/tmp")

# Parámetros de entrada (variables)

bucket_name = os.environ['BUCKET_NAME']
template_key = "files/plantillas/Fase1/formato-acueducto.xlsx"
output_key = "files/entregables/Fase1/output03.xlsx"

def insert_image(ws, cellNumber, imagen_path):
    img = Image(str(imagen_path))
    cell = ws[cellNumber]

    # Medidas de la celda
    col_width = ws.column_dimensions[cell.column_letter].width or 8   # ancho columna en unidades de Excel
    row_height = ws.row_dimensions[cell.row].height or 15             # alto fila en puntos

    # Conversión aproximada a píxeles
    max_width = col_width * 7
    max_height = row_height * 0.75

    # Escala manteniendo proporciones
    ratio = min(max_width / img.width, max_height / img.height)

    img.width = img.width * ratio
    img.height = img.height * ratio

    ws.add_image(img, cellNumber)

def normalizar_booleans(data_get):
    def convertir_valor(valor):
        if isinstance(valor, bool):  # True/False nativos de Python
            return "Si" if valor else "No"
        if isinstance(valor, str):  # "true"/"false" como string
            if valor.lower() == "true":
                return "Si"
            if valor.lower() == "false":
                return "No"
        return valor  # cualquier otro valor queda igual

    # Si es un dict, lo recorremos recursivamente
    if isinstance(data_get, dict):
        return {k: normalizar_booleans(v) for k, v in data_get.items()}
    # Si es una lista, también recursivamente
    if isinstance(data_get, list):
        return [normalizar_booleans(v) for v in data_get]
    
    return convertir_valor(data_get)



def lambda_handler(event, context):

    # Paths locales en Lambda (/tmp)
    template_path = TMP_DIR / "plantilla.xlsx"
    imagen_keys = event["attachments"]
    imagen_paths = [TMP_DIR / Path(k).name for k in imagen_keys]
    output_path = TMP_DIR / "output.xlsx"

    # Descargar archivos desde S3
    s3.download_file(bucket_name, template_key, str(template_path))
    #s3.download_file(bucket_name, json_key, str(json_path))
    
    #descargar imagenes
    for key, path in zip(imagen_keys, imagen_paths):
        s3.download_file(bucket_name, key, str(path))

    payload_data = event["payload"]["attributes"]
    json_data = normalizar_booleans(payload_data)

    # Cargar plantilla Excel
    wb = load_workbook(template_path)
    ws = wb.active  # hoja específica con wb["NombreHoja"] o activa con wb.active

    #Estos campos deben coincidir con los placeholders
    #campos_contexto son todos los keys de payload_data  
    campos_contexto = list(payload_data.keys())

    # Diccionario de contexto
    context = {f"{{{{{campo}}}}}": json_data.get(campo, "") for campo in campos_contexto}

    print(context)

    # Reemplazar variables en todas las celdas
    for row in ws.iter_rows():
        for cell in row:
            if isinstance(cell.value, str):
                for placeholder, value in context.items():
                    if placeholder in cell.value:
                        cell.value = cell.value.replace(placeholder, str(value))

    # Insertar imágenes (en celdas específicas)
    celdas_imagenes = ["B39", "C39", "D39", "E39", "B40", "C40", "D40", "E40", "B41", "C41", "D41", "E41", "B42", "C42", "D42", "E42"]

    #Ciclo para insertar las imagenes en las celdas disponibles
    for celda, imagen_path in zip(celdas_imagenes, imagen_paths):
        insert_image(ws, celda, imagen_path)

    # Guardar archivo final
    wb.save(output_path)
    # Subir resultado a S3
    s3.upload_file(str(output_path), bucket_name, output_key)

    return {
        "status": "ok",
        "output_file": f"s3://{bucket_name}/{output_key}"
    }


