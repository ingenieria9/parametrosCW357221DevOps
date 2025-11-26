import boto3
import json
import os
import re
import tempfile
from pathlib import Path
from botocore.exceptions import ClientError
from io import BytesIO
from datetime import datetime, timezone, timedelta

from openpyxl import load_workbook, Workbook
from openpyxl.drawing.image import Image

s3 = boto3.client("s3")
TMP_DIR = Path("/tmp")

bucket_name = os.environ["BUCKET_NAME"]

# Rutas conocidas de Lambda A
template_path_s3 = "files/plantillas/Fase3/"
output_path_s3 = "files/entregables/Fase3/"

template_name = {"puntos_medicion": "formato-acueducto-pm.xlsx",
                 "vrp-caudal-PLUM": "formato-acueducto-vrp-caudal-PLUM.xlsx",
                 "vrp-presion_caudal-PLUM": "formato-acueducto-vrp-presion_caudal-PLUM.xlsx",
                 "vrp-presion-Additel": "formato-acueducto-vrp-presion-Additel.xlsx",
                  "vrp-presion-PLUM":  "formato-acueducto-vrp-presion-PLUM.xlsx",
                   "camara": "formato-alcantarillado.xlsx"}

template_general = 'listado-senales'

celdas_imagenes_plantilla = {"puntos_medicion": ["B22", "C22", "D22", "E22","B23", "C23", "D23", "E23", "B24", "C24", "D24", "E24"],
                            "vrp-caudal-PLUM": ["B22", "C22", "D22", "E22","B23", "C23", "D23", "E23", "B24", "C24", "D24", "E24"],
                            "vrp-presion_caudal-PLUM": ["B24", "C24", "D24", "E24","B25", "C25", "D25", "E25", "B26", "C26", "D26", "E26"],
                            "vrp-presion-Additel": ["B27", "C27", "D27", "E27","B28", "C28", "D28", "E28", "B29", "C29", "D29", "E29"],
                            "vrp-presion-PLUM": ["B24", "C24", "D24", "E24","B25", "C25", "D25", "E25", "B26", "C26", "D26", "E26"], "camara": []}

# === Funciones auxiliares ===

def insert_image(ws, cellNumber, image_source):
    if isinstance(image_source, (str, Path)):
        img = Image(str(image_source))
    else:
        # asume BytesIO
        image_source.seek(0)
        img = Image(image_source)

    cell = ws[cellNumber]

    # Valores por defecto si no existen en la hoja
    # column_dimensions uses letters, row_dimensions uses numeric index
    col_letter = cell.column_letter
    row_idx = cell.row

    col_dim = ws.column_dimensions.get(col_letter)
    row_dim = ws.row_dimensions.get(row_idx)

    # ancho columna en "unidades Excel" -> aproximamos píxeles multiplicando por ~8
    col_width = col_dim.width if (col_dim and col_dim.width) else 8
    row_height = row_dim.height if (row_dim and row_dim.height) else 15  # puntos

    # Conversión aproximada (empírica): 1 unidad de columna ~ 8 píxeles, 1 punto ~ 1.33 px
    max_width_px = col_width * 8
    max_height_px = row_height * 1.33

    # Evitar división por 0
    if img.width == 0 or img.height == 0:
        ratio = 1.0
    else:
        ratio = min(max_width_px / img.width, max_height_px / img.height, 1.0)

    img.width = int(img.width * ratio)
    img.height = int(img.height * ratio)

    ws.add_image(img, cellNumber)

def normalizar_booleans(data):
    if isinstance(data, dict):
        return {k: normalizar_booleans(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [normalizar_booleans(v) for v in data]
    elif isinstance(data, bool):
        return "Si" if data else "No"
    elif isinstance(data, str):
        if data.lower() == "true": return "Si"
        if data.lower() == "false": return "No"
    elif data is None:
        return ""
    return data

def convertir_valores_fecha(data):
    def convertir(k, v):
        if "fecha" in k.lower():
            try:
                if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
                    ts = int(v) / 1000
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(timezone(timedelta(hours=-5)))
                    return dt.strftime("%Y-%m-%d %H:%M:%S")
            except:
                pass
        return v
    if isinstance(data, dict):
        return {k: convertir(k, convertir_valores_fecha(v)) for k, v in data.items()}
    elif isinstance(data, list):
        return [convertir_valores_fecha(v) for v in data]
    return data

def obtener_info_de_capa_principal(bucket_name, tipo_punto, GlobalID, CIRCUITO_ACU):
    prefix = f"ArcGIS-Data/Puntos/{CIRCUITO_ACU}/{GlobalID}_{tipo_punto}/Capa_principal/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        return {}
    json_files = [obj for obj in response["Contents"] if obj["Key"].lower().endswith(".json")]
    if not json_files:
        return {}

    latest_json = max(json_files, key=lambda x: x["LastModified"])["Key"]
    tmp_path = TMP_DIR / f"capa_principal_{GlobalID}.json"
    s3.download_file(bucket_name, latest_json, str(tmp_path))
    with open(tmp_path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except:
            return {}

def generar_hoja_desde_template(
    wb_final,
    payload_data,
    capa_principal_data,
    tipo_punto,
    imagen_keys,
    bucket_name,
    template_path_s3,
    template_name, code_key):
    """
    Genera una hoja nueva dentro del workbook consolidado usando la plantilla
    correspondiente al tipo de punto. Mantiene formato, merges, imágenes,
    bordes, fuentes y tamaños de celda, e inserta imágenes (tanto attachments como
    imágenes de Capa_principal).
    """

    #tipo_key = tipo_punto.lower()
    template_filename = template_name.get(code_key)
    if not template_filename:
        raise ValueError(f"No se encontró plantilla para tipo de punto: {tipo_punto}")

    template_key = template_path_s3 + template_filename

    # Descargar plantilla desde S3 a BytesIO
    template_stream = BytesIO()
    s3.download_fileobj(bucket_name, template_key, template_stream)
    template_stream.seek(0)

    # Cargar workbook de plantilla
    wb_template = load_workbook(template_stream)
    ws_template = wb_template.active

    # Obtener FID (convertir a string)
    fid = str(payload_data.get("FID_ELEM", "SIN_FID"))
    sheet_name = fid[:31] if fid else "SIN_FID"
    ws_new = wb_final.create_sheet(title=sheet_name)

    # === Copiar celdas y estilos ===
    for row_idx, row in enumerate(ws_template.iter_rows(), start=1):
        for col_idx, cell in enumerate(row, start=1):
            new_cell = ws_new.cell(row=row_idx, column=col_idx, value=cell.value)
            if cell.has_style:
                try:
                    new_cell.font = cell.font.copy()
                    new_cell.border = cell.border.copy()
                    new_cell.fill = cell.fill.copy()
                    new_cell.number_format = cell.number_format
                    new_cell.protection = cell.protection.copy()
                    new_cell.alignment = cell.alignment.copy()
                except Exception:
                    # en caso de que alguna copia falle, no rompemos la ejecución
                    pass

    # Copiar merges
    for merged_range in ws_template.merged_cells.ranges:
        ws_new.merge_cells(str(merged_range))

    # Copiar tamaños de columnas
    for col_letter, dim in ws_template.column_dimensions.items():
        if dim.width:
            ws_new.column_dimensions[col_letter].width = dim.width

    # Copiar alturas de filas
    for row_idx, dim in ws_template.row_dimensions.items():
        try:
            if dim.height:
                ws_new.row_dimensions[row_idx].height = dim.height
        except Exception:
            pass

    # === AJUSTE MANUAL DE CELDAS PARA IMÁGENES (requerido) ===
    if tipo_punto in ["puntos_medicion", "vrp"]:
        for col in ["B", "C", "D", "E"]:
            ws_new.column_dimensions[col].width = 40  # ancho ideal para imágenes

    # === Copiar formato condicional ===
    cf_template = ws_template.conditional_formatting
    for cf_range, rules in getattr(cf_template, "_cf_rules", {}).items():
        for rule in rules:
            try:
                ws_new.conditional_formatting.add(cf_range, rule)
            except Exception as e:
                print(f"No se pudo copiar formato condicional para {cf_range}: {e}")       


    # === Insertar imágenes propias de la plantilla (logos, etc.) ===
    # openpyxl stores images as Image objects in _images; extraemos su data si es posible.
    for img in getattr(ws_template, "_images", []):
        try:
            # intentamos obtener bytes si el objeto lo contiene
            img_bytes = None
            # si es Image cargada de archivo local, openpyxl Image tiene .ref o .path en algunas versiones
            try:
                img_bytes = BytesIO(img._data())
            except Exception:
                # fallback: si es Image referenciada por path
                try:
                    img_bytes = BytesIO(open(img.ref, "rb").read())
                except Exception:
                    img_bytes = None

            if img_bytes:
                new_img = Image(img_bytes)
                # Mantener ancla aproximada
                try:
                    new_img.anchor = img.anchor
                except Exception:
                    pass
                ws_new.add_image(new_img)
        except Exception:
            # no rompemos por una imagen de plantilla
            pass

    payload_data = convertir_valores_fecha(payload_data)
    capa_principal_data = convertir_valores_fecha(capa_principal_data)

    # === Reemplazar placeholders con datos ===
    def reemplazar_valor(texto):
        if not isinstance(texto, str):
            return texto
        pattern = re.compile(r"\{\{(.*?)\}\}")
        def repl(match):
            key = match.group(1).strip()
            # Buscar en payload primero, luego en capa principal
            if key in payload_data:
                return str(payload_data[key] if payload_data[key] is not None else "")
            if key in capa_principal_data:
                return str(capa_principal_data[key] if capa_principal_data[key] is not None else "")
            return match.group(0)
        return pattern.sub(repl, texto)

    for row in ws_new.iter_rows():
        for cell in row:
            if isinstance(cell.value, str):
                cell.value = reemplazar_valor(cell.value)

    # === Preparamos lista de celdas disponibles para imágenes según plantilla ===
    celdas_imagenes = celdas_imagenes_plantilla.get(code_key, [])

    # === Descargar imágenes (attachments + capa_principal) localmente/stream y luego insertarlas ===
    # imagen_keys puede contener rutas locales (files/...) o llaves S3. Detectamos S3 por que no empiezan con '/' ni 'tmp' ni 'files/temp-image-folder'
    imagen_streams = []
    for key in imagen_keys:
        # evitar None o strings vacíos
        if not key or not isinstance(key, str):
            continue
        # En algunos payloads attachments vienen concatenados a un string; intentar separar por comas si detectamos eso
        if "," in key and (key.startswith("CW") or key.startswith("files/")):
            # si es un string con varias keys separadas por comas, separar
            parts = [p.strip() for p in key.split(",") if p.strip()]
        else:
            parts = [key.strip()]

        for k in parts:
            # Si es ya una ruta local (por ejemplo 'files/temp-image-folder/ejemplo1.jpg') o startswith('/') usamos directo
            if k.startswith("/") or k.startswith("files/") or k.startswith("tmp/"):
                local = Path(TMP_DIR) / Path(k).name
                # si no existe, intentar descargar si parece S3 key (empieza por files/ suele ser key en S3)
                if not local.exists():
                    # intentar descargar desde S3 si la key existe
                    try:
                        s3.download_file(bucket_name, k, str(local))
                    except Exception:
                        # no se pudo descargar, saltar
                        continue
                imagen_streams.append(local)
            else:
                # Probablemente es una key de S3 (p.e. "ArcGIS-Data/...")
                local = TMP_DIR / Path(k).name
                try:
                    # si la key incluye prefijo tipo "CW357221-ArcGIS-Data/..." y viene con coma, limpiamos prefijo accidental
                    # A veces attachments traen "CW357221-<key>", si detectamos '-' antes de 'ArcGIS-Data' intentamos extraer la parte desde 'ArcGIS-Data'
                    if "-" in k and "ArcGIS-Data" in k:
                        parts_dash = k.split("-", 1)
                        candidate = parts_dash[1]
                        # si candidate existe en s3, preferirlo
                        try:
                            s3.head_object(Bucket=bucket_name, Key=candidate)
                            k_to_use = candidate
                        except Exception:
                            k_to_use = k
                    else:
                        k_to_use = k

                    s3.download_file(bucket_name, k_to_use, str(local))
                    imagen_streams.append(local)
                except Exception:
                    # como fallback intentar descargar directamente con la key original
                    try:
                        s3.download_file(bucket_name, k, str(local))
                        imagen_streams.append(local)
                    except Exception:
                        # no pudimos descargar la imagen, la ignoramos
                        continue

    # === Insertar imágenes en las celdas asignadas ===
    for celda, img_src in zip(celdas_imagenes, imagen_streams):
        try:
            insert_image(ws_new, celda, img_src)
        except Exception:
            # si falla una imagen, seguimos con las siguientes
            continue

    return ws_new


def generar_resumen_desde_template(wb_final, lista_puntos, bucket_name, template_path_s3):
    """
    Genera la hoja 'Resumen' copiando encabezados/estilos desde la plantilla
    y replicando una fila que contiene los placeholders {{punto.xxx}}.
    """

    template_filename = "listado-senales.xlsx"
    template_key = f"{template_path_s3}{template_filename}"

    # Descargar plantilla desde S3
    template_stream = BytesIO()
    s3.download_fileobj(bucket_name, template_key, template_stream)
    template_stream.seek(0)

    wb_template = load_workbook(template_stream)
    ws_template = wb_template.active

    # Crear hoja final
    ws_resumen = wb_final.create_sheet("Resumen")

    # Copiar TODA la hoja de plantilla (encabezados, estilos, merges)
    for row_idx, row in enumerate(ws_template.iter_rows(), start=1):
        for col_idx, cell in enumerate(row, start=1):
            new_cell = ws_resumen.cell(row=row_idx, column=col_idx, value=cell.value)

            if cell.has_style:
                new_cell.font = cell.font.copy()
                new_cell.border = cell.border.copy()
                new_cell.fill = cell.fill.copy()
                new_cell.number_format = cell.number_format
                new_cell.alignment = cell.alignment.copy()

    # Copiar merges
    for merge in ws_template.merged_cells.ranges:
        ws_resumen.merge_cells(str(merge))

    # Buscar fila plantilla con placeholders
    fila_template = None
    for row_idx, row in enumerate(ws_template.iter_rows(), start=1):
        row_text = " ".join([str(c.value) for c in row if isinstance(c.value, str)])
        if "{{punto." in row_text:
            fila_template = row_idx
            break

    if not fila_template:
        print("⚠ No se encontró fila plantilla con placeholders {{punto.xxx}}.")
        return ws_resumen

    # Para evitar que la fila plantilla original quede vacía en la salida,
    # se elimina esa fila y se reconstruye con datos reales
    ws_resumen.delete_rows(fila_template)

    # Insertar filas reemplazando los placeholders
    for punto in lista_puntos:
        ws_resumen.insert_rows(fila_template)

        for col_idx, cell_template in enumerate(ws_template[fila_template], start=1):
            valor = cell_template.value

            # Reemplazar placeholders
            if isinstance(valor, str):
                valor = (
                    valor.replace("{{punto.FID_ELEM}}", str(punto.get("FID_ELEM", "")))
                         .replace("{{punto.VARIABLES_MEDICION}}", str(punto.get("VARIABLES_MEDICION", "")))
                         .replace("{{punto.OBSERV_ACU}}", str(punto.get("OBSERV_ACU", "")))
                         .replace("{{punto.CRITERIO_ACU}}", str(punto.get("CRITERIO_ACU", "")))
                )

            new_cell = ws_resumen.cell(row=fila_template, column=col_idx, value=valor)

            # Copiar estilos
            if cell_template.has_style:
                new_cell.font = cell_template.font.copy()
                new_cell.border = cell_template.border.copy()
                new_cell.fill = cell_template.fill.copy()
                new_cell.number_format = cell_template.number_format
                new_cell.alignment = cell_template.alignment.copy()

        fila_template += 1

    return ws_resumen


# === Lambda Handler principal que genera un workbook con una hoja por punto ===
def lambda_handler(event, context):
    cod = event["payload"].get("COD", "")
    numero_consolidado = int(event["payload"].get("numero_consolidado", 0))
    circuito_acu = event["payload"].get("CIRCUITO_ACU", "").replace(" ", "_")

    # --- SISTEMA DE LOCK POR CIRCUITO ---
    lock_key = f"locks_formato/fase3/{circuito_acu}.lock"

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
        print(f"Lock creado para {circuito_acu}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'PreconditionFailed':
            print(f"Otro proceso ya tiene lock para {circuito_acu}")
            return {
                "statusCode": 200,
                "body": f"Lambda saltada: lock activo para {circuito_acu}"
            }
        raise    

    try:
        # === Buscar puntos con carpeta Fase3 ===
        prefix = f"ArcGIS-Data/Puntos/{circuito_acu}/"
        paginator = s3.get_paginator("list_objects_v2")

        subcarpetas_puntos = set()
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
            for c in page.get("CommonPrefixes", []):
                subcarpetas_puntos.add(c["Prefix"])

        puntos_fase3 = []
        for subfolder in subcarpetas_puntos:
            fase3_prefix = f"{subfolder}Fase3/"
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=fase3_prefix)
            if "Contents" not in response:
                continue
            json_files = [obj for obj in response["Contents"] if obj["Key"].lower().endswith(".json")]
            if not json_files:
                continue
            # Tomar el JSON más reciente de Fase3
            latest_json = max(json_files, key=lambda x: x["LastModified"])["Key"]
            puntos_fase3.append(latest_json)

        if not puntos_fase3:
            return {"status": "no_points_found", "circuito": circuito_acu}

        print(f"Se encontraron {len(puntos_fase3)} puntos Fase3 para {circuito_acu}")

        wb_final = Workbook()

        # Quitar la hoja creada por defecto para evitar interferencias
        ws_default = wb_final.active
        wb_final.remove(ws_default)
        #ws_default = wb_final.active
        #ws_default.title = "Resumen"

        listado_senales = []

        for json_key in puntos_fase3:
            tmp_json = TMP_DIR / f"punto_{Path(json_key).name}"
            s3.download_file(bucket_name, json_key, str(tmp_json))
            with open(tmp_json, "r", encoding="utf-8") as f:
                try:
                    payload_data = json.load(f)
                except Exception:
                    payload_data = {}

            tipo_punto = payload_data.get("TIPO_PUNTO", "").lower()
            GlobalID = payload_data.get("PARENT_ID")
            fid = payload_data.get("FID_ELEM", "SIN_FID")
            VARIABLES_MEDICION = payload_data.get("VARIABLES_MEDICION", "")
            EQUIPO__DATALOGGER_INSTALADOS = payload_data.get("EQUIPO__DATALOGGER_INSTALADOS", "")

            if tipo_punto == "vrp":
                code_key = tipo_punto + "-" + VARIABLES_MEDICION + "-" + EQUIPO__DATALOGGER_INSTALADOS
            else:
                code_key = tipo_punto

            # Obtener capa principal (atributos + posiblemente imágenes en esa carpeta)
            capa_principal_data = obtener_info_de_capa_principal(bucket_name, tipo_punto, GlobalID, circuito_acu)

            # Para pagina inicial "LSE"
            listado_senales.append({
                "FID_ELEM": capa_principal_data.get("FID_ELEM"),
                "VARIABLES_MEDICION": capa_principal_data.get("VARIABLES_MEDICION"),
                "OBSERV_ACU": capa_principal_data.get("OBSERV_ACU"),
                "CRITERIO_ACU": capa_principal_data.get("CRITERIO_ACU")
            })

            # Buscar imágenes dentro de Fase3
            folder = f"ArcGIS-Data/Puntos/{circuito_acu}/{GlobalID}_{tipo_punto}/Fase3/"
            resp = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
            imagen_keys = []
            if "Contents" in resp:
                imagen_keys = [x["Key"] for x in resp["Contents"] if x["Key"].lower().endswith((".jpg", ".jpeg", ".png"))]

            # Buscar imágenes dentro de Capa_principal y agregarlas
            folder_cp = f"ArcGIS-Data/Puntos/{circuito_acu}/{GlobalID}_{tipo_punto}/Capa_principal/"
            resp_cp = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_cp)
            if "Contents" in resp_cp:
                imagen_keys_cp = [x["Key"] for x in resp_cp["Contents"] if x["Key"].lower().endswith((".jpg", ".jpeg", ".png"))]
                # unir evitando duplicados
                for k in imagen_keys_cp:
                    if k not in imagen_keys:
                        imagen_keys.append(k)

            # También soportar attachments que vengan dentro del JSON payload (campo attachments o attachments list)
            attachments = []
            if isinstance(payload_data.get("attachments"), list):
                attachments = payload_data.get("attachments")
            elif isinstance(payload_data.get("attachments"), str):
                # si vienen como "a,b,c" intentar separar
                attachments = [p.strip() for p in payload_data.get("attachments").split(",") if p.strip()]

            # unir attachments (siempre como S3 keys o rutas)
            for a in attachments:
                if a not in imagen_keys:
                    imagen_keys.append(a)

            # Normalmente generar_hoja_desde_template descargará las imágenes desde S3 según las keys
            generar_hoja_desde_template(
                wb_final,
                payload_data,
                capa_principal_data,
                tipo_punto,
                imagen_keys,
                bucket_name,
                template_path_s3,
                template_name, code_key
            )

        generar_resumen_desde_template(wb_final,listado_senales,bucket_name,template_path_s3)

        #if ws_resumen.max_row == 1 and ws_resumen.max_column == 1:
        #    # si no quedó contenido en hoja resumen, borrarla
        #    try:
        #        wb_final.remove(ws_default)
        #    except Exception:
        #        pass

        output_filename = f"MPH-EJ-0601-{cod}-F03-ACU-LSE-001.xlsx"
        output_key = f"{output_path_s3}ACU/CIR/{output_filename}"
        output_path = TMP_DIR / output_filename
        wb_final.save(output_path)
        s3.upload_file(str(output_path), bucket_name, output_key)

        return {
            "status": "ok",
            "circuito": circuito_acu,
            "puntos_total": len(puntos_fase3),
            "output_file": f"s3://{bucket_name}/{output_key}"
        }

    finally:
        # --- Liberar el lock ---
        try:
            s3.delete_object(Bucket=bucket_name, Key=lock_key)
            print(f"Lock liberado para {circuito_acu}")
        except Exception as e:
            print(f"Error al eliminar lock para {circuito_acu}: {e}")