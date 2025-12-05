import json
import os
import boto3
from datetime import datetime, timedelta
from DB_capa_principal import build_bulk_upsert_sql, convertir_valores_fecha
client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]
fecha_ini_cir_list = []

# funcion para invicar lambda de base de datos
def invoke_lambda_db_b(payload, FunctionName):
    response = client_lambda_db.invoke(
        FunctionName=FunctionName,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )
    return response

def default_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()
    return str(o)

def sql_value(v):
    if v is None:
        return "NULL"
    if isinstance(v, datetime):
        return f"'{v.isoformat()}'"
    return f"'{v}'"
        
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
        return json.loads(result)
    except json.JSONDecodeError:
        return {"raw_response": result}   

def db_upsert_fase_3_a_data(json_data):

    #verifica si es string
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_3_fields = [
        "PARENT_ID","FID_ELEM" ,"IDENTIFICADOR_DATALOGGER" ,"TIPO_PUNTO","EQUIPO__DATALOGGER_INSTALADOS",
        "CIRCUITO_ACU","El_punto_requiere_fase_3","FECHA_FASE3","MEDIDA_PRESION",
        "MEDIDA_PRESION2","MEDIDA_CAUDAL","MEDIDA_VELOCIDAD","MEDIDA_NIVEL",
        "REFERENCIA_PRESION","REFERENCIA_PRESION_2","CARGA_BATERIA","VARIABLES_MEDICION",
        "CAMPO_EXTRA_1", "CAMPO_EXTRA_2","CHECK_REC","TIEMPO_MUSTEREO","CAMPO_EXTRA_7_LIST",
        "CAMPO_EXTRA_8_TEXT","CAMPO_EXTRA_4_NUM","CAMPO_EXTRA_3_NUM",
        "MEDIDA_PRESION_DES","REFERENCIA_PRESION_DES","MEDIDA_PRESION2_DES","REFERENCIA_PRESION_2_DES",
        "MEDIDA_NIVEL_DES","MEDIDA_VEL_DES","MEDIDA_VEL_DES","CAMPO_EXTRA_6_TEXT",
        "SENSORES_LIMPIOS","ALMACENAMIENTO_DATOS_LOCALES","TRANSMISION_DATOS",
        
    ]

    all_rows = []
    #Caso: dict con múltiples claves
    for key, items in json_data.items():
        try:
            for item in items:  # porque item es una lista con objetos
                payload = item.get("payload", {})
                attributes = payload  # los campos están dentro de payload

                attributes = convertir_valores_fecha(attributes)
                capa_principal_values = {}

                for field in fase_3_fields:
                    value = attributes.get(field, None)
                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        value_str= str(value)            
                        # Si parece un número con coma decimal → cambiar a punto
                        # Ejemplos: "12,5" → "12.5"
                        if "," in value_str and value_str.replace(",", ".", 1).replace(".", "", 1).isdigit():
                            value = value_str.replace(",", ".")
                        
                        # Si es numérico legítimo con punto o sin punto, lo dejamos tal cual
                        elif value_str.replace(".", "", 1).isdigit():
                            value = value_str
                            
                        # Si no es numérico, lo dejamos igual
                        else:
                            value = value_str
                            
                    capa_principal_values[field] = value

                all_rows.append(capa_principal_values)
        except Exception as e:
            print(f"Error procesando registro {key}: {e}")
            
    upsert_sql = build_bulk_upsert_sql("fase_3_a_data", all_rows, "PARENT_ID")

    print("PAYLOAD FASE 3_A data: ", upsert_sql)

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    return payload_db


def db_upsert_fase_3_a_status(json_data):

    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_3_fields = [
        "PARENT_ID","FID_ELEM" ,"IDENTIFICADOR_DATALOGGER","TIPO_PUNTO",
        "EQUIPO__DATALOGGER_INSTALADOS","CIRCUITO_ACU","FECHA_FASE3",
        "MEDIDA_PRESION","CAMPO_EXTRA_8_TEXT","MEDIDA_PRESION2",
        "MEDIDA_CAUDAL","MEDIDA_VELOCIDAD","MEDIDA_NIVEL",
        "REFERENCIA_PRESION","REFERENCIA_PRESION_2","CARGA_BATERIA",
        "VARIABLES_MEDICION","CAMPO_EXTRA_1", "CAMPO_EXTRA_2",
        "CHECK_REC","TIEMPO_MUSTEREO","CAMPO_EXTRA_7_LIST",
        "CAMPO_EXTRA_6_TEXT","SENSORES_LIMPIOS",
        "ALMACENAMIENTO_DATOS_LOCALES","TRANSMISION_DATOS"
    ]

    all_rows = []      # filas para UPSERT
    update_rows = []   # filas para UPDATE por desinstalación

    # Procesamiento fila por fila
    for key, items in json_data.items():
        try:
            for item in items:
                payload = item.get("payload", {})
                attributes = convertir_valores_fecha(payload)

                capa_principal_values = {}

                for field in fase_3_fields:
                    value = attributes.get(field, None)

                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        value_str = str(value)
                        if "," in value_str and value_str.replace(",", ".", 1).replace(".", "", 1).isdigit():
                            value = value_str.replace(",", ".")
                        elif value_str.replace(".", "", 1).isdigit():
                            value = value_str
                        else:
                            value = value_str

                    capa_principal_values[field] = value

                # --------------------------------------------------------
                # EVALUAR SOLO ESTA FILA
                # --------------------------------------------------------
                if capa_principal_values.get("CAMPO_EXTRA_7_LIST", "").lower() == "desinstaclacion":
                    # Esta fila NO va al UPSERT
                    update_rows.append({
                        "IDENTIFICADOR_DATALOGGER": capa_principal_values.get("IDENTIFICADOR_DATALOGGER"),
                        "CAMPO_EXTRA_8_TEXT": capa_principal_values.get("CAMPO_EXTRA_8_TEXT", "NULL")
                    })
                else:
                    # Esta fila sí va al UPSERT
                    all_rows.append(capa_principal_values)

        except Exception as e:
            print(f"Error procesando registro {key}: {e}")

    # ------------------------------------------------------------
    # Generar SQL de UPDATE por desinstalación (si existen)
    # ------------------------------------------------------------
    update_sql = ""
    if update_rows:
        update_statements = []
        for row in update_rows:
            update_statements.append(f"""
                UPDATE fase_3_a_status 
                SET "CAMPO_EXTRA_8_TEXT" = '{row["CAMPO_EXTRA_8_TEXT"]}'
                WHERE "IDENTIFICADOR_DATALOGGER" = '{row["IDENTIFICADOR_DATALOGGER"]}';
            """)
        update_sql = "\n".join(update_statements)

    # ------------------------------------------------------------
    # Generar SQL de UPSERT (solo filas permitidas)
    # ------------------------------------------------------------
    if all_rows:
        upsert_sql = build_bulk_upsert_sql("fase_3_a_status", all_rows, "IDENTIFICADOR_DATALOGGER")
    else:
        upsert_sql = ""   # no hay filas válidas

    # ------------------------------------------------------------
    # Unimos ambos SQL en uno solo
    # ------------------------------------------------------------
    final_sql = ""
    if upsert_sql:
        final_sql += upsert_sql + "\n"
    if update_sql:
        final_sql += update_sql

    print("SQL FINAL GENERADO:\n", final_sql)

    return {
        "queryStringParameters": {
            "query": final_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }


def db_upsert_fase_3_a_status_b(json_data):

    #verifica si es string
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_3_fields = [
      
        "PARENT_ID","FID_ELEM" ,"IDENTIFICADOR_DATALOGGER" ,"TIPO_PUNTO","EQUIPO__DATALOGGER_INSTALADOS",
        "CIRCUITO_ACU","FECHA_FASE3","MEDIDA_PRESION","CAMPO_EXTRA_8_TEXT",
        "MEDIDA_PRESION2","MEDIDA_CAUDAL","MEDIDA_VELOCIDAD","MEDIDA_NIVEL",
        "REFERENCIA_PRESION","REFERENCIA_PRESION_2","CARGA_BATERIA","VARIABLES_MEDICION",
        "CAMPO_EXTRA_1", "CAMPO_EXTRA_2","CHECK_REC","TIEMPO_MUSTEREO","CAMPO_EXTRA_7_LIST",
        "CAMPO_EXTRA_6_TEXT","SENSORES_LIMPIOS","ALMACENAMIENTO_DATOS_LOCALES","TRANSMISION_DATOS"
    ]

    all_rows = []
    #Caso: dict con múltiples claves
    for key, items in json_data.items():
        try:
            for item in items:  # porque item es una lista con objetos
                payload = item.get("payload", {})
                attributes = payload  # los campos están dentro de payload

                attributes = convertir_valores_fecha(attributes)
                capa_principal_values = {}

                for field in fase_3_fields:
                    value = attributes.get(field, None)
                    
                    
                    if value is None or str(value).strip().lower() == 'none':
                        value = "NULL"
                    else:
                        
                        value_str= str(value)            
                        # Si parece un número con coma decimal → cambiar a punto
                        # Ejemplos: "12,5" → "12.5"
                        if "," in value_str and value_str.replace(",", ".", 1).replace(".", "", 1).isdigit():
                            value = value_str.replace(",", ".")
                        
                        # Si es numérico legítimo con punto o sin punto, lo dejamos tal cual
                        elif value_str.replace(".", "", 1).isdigit():
                            value = value_str
                            
                        # Si no es numérico, lo dejamos igual
                        else:
                            value = value_str
                    capa_principal_values[field] = value

                all_rows.append(capa_principal_values)
        except Exception as e:
            print(f"Error procesando registro {key}: {e}")
    
    
   # print("DENTRO UPSERT FASE 3 STATUS",all_rows)        
    upsert_sql = build_bulk_upsert_sql("fase_3_a_status", all_rows, "IDENTIFICADOR_DATALOGGER")

    print("PAYLOAD FASE 3_A STATUS: ", upsert_sql)

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    return payload_db



def db_fase_3_a_b_trazabilidad_mediciones(parents_relation):

    # Convertir la lista de ids a formato ('', '', '') para postgress
    global_ids_tupla = tuple(parents_relation)
    

    # 1. BUSQUEDA EN BASE DE DATOS: puntos capa principal
    # Por cada punto en capa principal busca que
    # ["TIPO_PUNTO"] de [CIRCUITO_ACU] tiene [HABILITADO_FASE3] y asigna esta 
    # cuenta en una nueva variable [habilitados_fase3]
    #EJEMPLO: 
    #{"CIRCUITO_ACU": "PRUEBA", "TIPO_PUNTO": "vrp", "FECHA_FASE3": "2025-12-02 12:08:31", "TRAZABILIDAD": null, "habilitados_fase3": 2, "total_hab_fase3": 4}]'}
    
    query = f"""
        
    SELECT 
        f3."CIRCUITO_ACU",
        f3."TIPO_PUNTO",
        f3."FECHA_FASE3",
        MAX(f3."TRAZABILIDAD") AS "TRAZABILIDAD",
        COUNT(pcp.*) AS habilitados_fase3,
        (
            SELECT COUNT(*)
            FROM puntos_capa_principal p2
            WHERE p2."CIRCUITO_ACU" = f3."CIRCUITO_ACU"
            AND p2."HABILITADO_FASE3" = 1
        ) AS total_hab_fase3
    FROM fase_3_a_data f3
    LEFT JOIN puntos_capa_principal pcp
        ON pcp."CIRCUITO_ACU" = f3."CIRCUITO_ACU"
        AND pcp."TIPO_PUNTO" = f3."TIPO_PUNTO"
        AND pcp."HABILITADO_FASE3" = 1
    WHERE f3."PARENT_ID" IN {global_ids_tupla}
    AND f3."TRAZABILIDAD" IS NULL
    GROUP BY 
        f3."CIRCUITO_ACU",
        f3."TIPO_PUNTO",
        f3."FECHA_FASE3";

    """

   
    print("Query fase 3, capa principal", query)
    
    payload_f3_db = {
        "queryStringParameters": {
            "query": query,
            "db_name": "parametros",
            "time_column": "FECHA_FASE3",
        }
    }
    
    # respuesta de la consulta a la base de datos de capa principal
    filas = invoke_lambda_db(payload_f3_db, DB_ACCESS_LAMBDA_ARN)  # ← 1ra llamada
    print("Filas de 1er llamado:", filas)
    
    # obtener el body como string
    body_str = filas["body"]
    
    # convertir a Python (lista de dicts)
    body_json = json.loads(body_str)
    print("BODY JSON:",body_json)

    # 2. OBTENER TRAZABILIDAD EXISTENTE DE fase_3_a_b_trazabilidad_mediciones
    # Va a la base de datos de fase_3_a_b_trazabilidad_mediciones (Unique Id : Circuito)
    
    # del payload recibido:
    cir_tupla = tuple(f["CIRCUITO_ACU"] for f in body_json)
    
    # construir la lista para IN
    in_list = ", ".join(f"'{c}'" for c in cir_tupla)
    
    # si cir_tupla está vacío, no hay nada que consultar
    if not cir_tupla:
        
        print("cir_tupla vacío: no se consulta la tabla de trazabilidad.")
        trazabilidad_dict = {}  # sin registros previos
    
    # obtener trazabilidad existente de fase_3_a_b_trazabilidad_mediciones    
    else:
        
        query_trazabilidad = f"""
            SELECT 
                "CIRCUITO_ACU",
                "NUMERO_PUNTOS",
                "NUMERO_VRP"
            FROM fase_3_a_b_trazabilidad_mediciones
            WHERE "CIRCUITO_ACU" IN ({in_list});
        """

        payload_traza_db = {
            "queryStringParameters": {
                "query": query_trazabilidad,
                "db_name": "parametros",
                "time_column": "FECHA_INICIO_INSTALACION",
            }
        }
        
        trazabilidad = invoke_lambda_db(payload_traza_db, DB_ACCESS_LAMBDA_ARN)
        print("PAYLOAD TRAZABILIDAD: ", trazabilidad)

        body_trazabilidad = trazabilidad["body"]

        try:
            trazabilidad_json = json.loads(body_trazabilidad)
        except json.JSONDecodeError:
            trazabilidad_json = []  # si body no es JSON válido

        # Asegurar que es una lista de dicts
        if not isinstance(trazabilidad_json, list):
            trazabilidad_json = []

        # Filtrar solo diccionarios (evitar strings dentro de la lista)
        trazabilidad_json = [t for t in trazabilidad_json if isinstance(t, dict)]

        trazabilidad_dict = {
            t["CIRCUITO_ACU"]: t
            for t in trazabilidad_json
        }
        print("trazabilidad dict: ", trazabilidad_dict)

        

    resultados = []
    upserts = []   #Aquí guardamos todos los UPSERT SQL
    
    
    #Procesar cada punto de capa principal  
    for fila in body_json:

        circuito = fila["CIRCUITO_ACU"]
        tipo_punto = fila["TIPO_PUNTO"]
        fecha_fase3 = fila["FECHA_FASE3"]
        traza_total_habilitados = fila["total_hab_fase3"]
        suma_puntos = 0
        
        # Fecha se va a recibir como string -> convertir a formato fecha
        if fecha_fase3 and isinstance(fecha_fase3, str):
            try:
                fecha_fase3 = datetime.fromisoformat(fecha_fase3)
            except ValueError:
                fecha_fase3 = datetime.strptime(fecha_fase3, "%Y-%m-%d %H:%M:%S")
                
        habilitados = fila["habilitados_fase3"]
        
        # Campo que cambia por tipo de punto
        if tipo_punto == "vrp":
            campo_numero = "NUMERO_VRP"
        elif tipo_punto == "puntos_medicion":
            campo_numero = "NUMERO_PUNTOS"
        else:
            campo_numero = None

        # Registro existente o nuevo
        registro = trazabilidad_dict.get(circuito)

        if registro and campo_numero:
            
            numero_actual = (registro.get(campo_numero) or 0)
            numero_vrp = (registro.get("NUMERO_VRP") or 0)
            numero_puntos = (registro.get("NUMERO_PUNTOS") or 0)
            
            numero_total = numero_vrp + numero_puntos
            suma_puntos +=  numero_total
            
        else:
            numero_actual = 0
            suma_puntos = 0

        # Plantilla para insertar en la base de datos de fase_3_a_b_trazabilidad
        resultado = {
            "CIRCUITO_ACU": circuito,
            campo_numero: None,
            "FECHA_INICIO_INSTALACION": None,
            "FECHA_FIN_INSTALACION": None,
            "FECHA_INICIO_MEDICION": None,
            "FECHA_FIN_MEDICION": None
        }

        
        # LÓGICA para llenar las columnas de fase_3_a_b_trazabilidad_mediciones
       
        if suma_puntos == 0:
            resultado[campo_numero] = 1
            suma_puntos += 1
            resultado["FECHA_INICIO_INSTALACION"] = fecha_fase3
            print("ENTRA A CONDICIONAL SUMA PUNTOS = 0")
            
            # Actualizar trazabilidad_dict
            if circuito not in trazabilidad_dict:
                trazabilidad_dict[circuito] = {"NUMERO_PUNTOS": 0, "NUMERO_VRP": 0} 
            
            trazabilidad_dict[circuito][campo_numero] = 1
            
            
        elif numero_actual < habilitados:
            
            
            resultado[campo_numero] = numero_actual + 1
            suma_puntos += 1
            print("SUMA PUNTOS:" ,suma_puntos )
            
            # Actualizar trazabilidad_dict 
            trazabilidad_dict[circuito][campo_numero] = numero_actual + 1
            

            if suma_puntos == traza_total_habilitados:
                
                #resultado[campo_numero] = numero_actual
                resultado["FECHA_FIN_INSTALACION"] = fecha_fase3
                #resultado["FECHA_INICIO_INSTALACION"] = fecha_inicio_prev
                print("ENTRA A CONDICIONAL SUMA PUNTOS = Total hab")

                dia_siguiente = (fecha_fase3 + timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                print(dia_siguiente)
                resultado["FECHA_INICIO_MEDICION"] = dia_siguiente + timedelta(hours=5)
                resultado["FECHA_FIN_MEDICION"] = dia_siguiente + timedelta(days=7) + timedelta(hours=5)

        resultados.append(resultado)
        print("Suma puntos:" , suma_puntos)
        print("RESULTADO: ", resultado)
      
        # Construir el UPSERT SQL por cada fila
    
        numero_val = resultado[campo_numero]

        upsert_sql = f"""
            INSERT INTO fase_3_a_b_trazabilidad_mediciones (
                "CIRCUITO_ACU",
                "{campo_numero}",
                "FECHA_INICIO_INSTALACION",
                "FECHA_FIN_INSTALACION",
                "FECHA_INICIO_MEDICION",
                "FECHA_FIN_MEDICION"
            )
            VALUES (
                '{circuito}',
                {numero_val},
                {sql_value(resultado["FECHA_INICIO_INSTALACION"])},
                {sql_value(resultado["FECHA_FIN_INSTALACION"])},
                {sql_value(resultado["FECHA_INICIO_MEDICION"])},
                {sql_value(resultado["FECHA_FIN_MEDICION"])}
            )
            ON CONFLICT ("CIRCUITO_ACU")
            DO UPDATE SET 
                "{campo_numero}" = EXCLUDED."{campo_numero}",
                "FECHA_FIN_INSTALACION" = EXCLUDED."FECHA_FIN_INSTALACION",
                "FECHA_INICIO_MEDICION" = EXCLUDED."FECHA_INICIO_MEDICION",
                "FECHA_FIN_MEDICION" = EXCLUDED."FECHA_FIN_MEDICION";
        """


        upserts.append(upsert_sql)


    #  Ejecutar llamada a la lambda (por paquete con todos los puntos obtenidos
    # de capa principal)

    upserts_combined = "\n".join(upserts)
    payload_upsert_db = {
        "queryStringParameters": {
            "query": upserts_combined,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    print("PAYLOAD PARA TRAZABILIDAD: ", upsert_sql)
    invoke_lambda_db(payload_upsert_db, DB_ACCESS_LAMBDA_ARN)  # ← 3ra llamada total


    #ACTUALIZACION DE TRAZABILIDAD EN fase_3_a_data:
    
    #primero revisamos si los puntos se agregaron correctamente en fase_3_a_data
    payload = db_select_puntos_fase3_a_data(parents_relation)
    respuesta = invoke_lambda_db(payload,DB_ACCESS_LAMBDA_ARN)
    
    # obtener el body como string
    respuesta_str = respuesta["body"]
    
    # convertir a Python (lista de dicts)
    respuesta_json = json.loads(respuesta_str)
    print("respuesta_json: ",respuesta_json)
    puntos_en_fase_3_a_data = []
    for punto in respuesta_json:
        puntos_en_fase_3_a_data.append(punto["PARENT_ID"])
        
    
    payload_trazabilidad_fase_3 = db_update_trazabilidad(puntos_en_fase_3_a_data)
    invoke_lambda_db(payload_trazabilidad_fase_3, DB_ACCESS_LAMBDA_ARN)

    
    return resultados

    
def db_select_puntos_fase3_a_data(parents_relation):
    
    # Convertirlos a formato ('', '', '') para postgress
    global_ids_tupla = tuple(parents_relation)
    global_list = ", ".join(f"'{c}'" for c in global_ids_tupla)
    
    if not global_ids_tupla:
        return  # o manejar el caso vacío
        # Query para actualizar
        
    select_sql = f"""
            SELECT *
            FROM fase_3_a_data f
            WHERE f."PARENT_ID" IN ({global_list});
            """
            
    payload_db = {
        "queryStringParameters": {
            "query": select_sql,
            "db_name": "parametros",
            "time_column": "FECHA_FASE3"
        }
    }

    return payload_db
            
def db_update_trazabilidad(parents_relation):

    # Convertirlos a formato ('', '', '') para postgress
    global_ids_tupla = tuple(parents_relation)
    global_list = ", ".join(f"'{c}'" for c in global_ids_tupla)
    
    """
    Actualiza HABILITADO_FASE3 en puntos_capa_principal usando:
    1. habilitado_medicion de fase_1 (prioridad)
    2. si fase_1 es 0, NULL o vacío → usar habilitado_medicion de fase_2
    """
    if not global_ids_tupla:
        return  # o manejar el caso vacío
        # Query para actualizar
    
    update_sql = f"""
            UPDATE fase_3_a_data f
            SET "TRAZABILIDAD" = 1
            WHERE f."PARENT_ID" IN ({global_list});
            """


    print("PAYLOAD FOR TRAZABILIDAD: ", update_sql)

    #print("update_habilitado_fase3:", update_sql)

    payload_db = {
        "queryStringParameters": {
            "query": update_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION"
        }
    }

    return payload_db    