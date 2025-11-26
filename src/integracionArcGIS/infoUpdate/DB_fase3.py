import json
import os
import boto3
from datetime import datetime, timedelta
from DB_capa_principal import build_bulk_upsert_sql, convertir_valores_fecha
client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]

# funcion para invicar lambda de base de datos
def invoke_lambda_db(payload, FunctionName):
    response = client_lambda_db.invoke(
        FunctionName=FunctionName,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8')
    )
   

def db_upsert_fase_3_a_data(json_data):

    #verifica si es string
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_3_fields = [
        "PARENT_ID","FID_ELEM" ,"IDENTIFICADOR_DATALOGGER" ,"TIPO_PUNTO","EQUIPO_DATALOGGER_INSTALADOS",
        "CIRCUITO_ACU","El_punto_requiere_fase_3","FECHA_FASE3","MEDIDA_PRESION",
        "MEDIDA_PRESION2","MEDIDA_CAUDAL","MEDIDA_VELOCIDAD","MEDIDA_NIVEL",
        "REFERENCIA_PRESION","REFERENCIA_PRESION_2","CARGA_BATERIA","VARIABLES_MEDICION",
        "CAMPO_EXTRA_1", "CAMPO_EXTRA_2" 
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
                        value = str(value)
                    capa_principal_values[field] = value

                all_rows.append(capa_principal_values)
        except Exception as e:
            print(f"Error procesando registro {key}: {e}")
            
    upsert_sql = build_bulk_upsert_sql("fase_3_a_data", all_rows, "PARENT_ID")

    

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    return payload_db

def db_upsert_fase_3_a_status(json_data):

    #verifica si es string
    if isinstance(json_data, str):
        json_data = json.loads(json_data)

    fase_3_fields = [
      
                "FID_ELEM","IDENTIFICADOR_DATALOGGER","TIPO_PUNTO",
                "EQUIPO__DATALOGGER_INSTALADOS" ,"CIRCUITO_ACU" ,
                "FECHA_FASE3" ,"MEDIDA_PRESION","MEDIDA_PRESION_2",
                "MEDIDA_CAUDAL","MEDIDA_VELOCIDAD","MEDIDA_NIVEL",
                "REFERENCIA_PRESION","REFERENCIA_PRESION_2","CARGA_BATERIA",
                "VARIABLES_MEDICION" 
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
                        value = str(value)
                    capa_principal_values[field] = value

                all_rows.append(capa_principal_values)
        except Exception as e:
            print(f"Error procesando registro {key}: {e}")
    
   # print("DENTRO UPSERT FASE 3 STATUS",all_rows)        
    upsert_sql = build_bulk_upsert_sql("fase_3_a_status", all_rows, "IDENTIFICADOR_DATALOGGER")

    

    payload_db = {
        "queryStringParameters": {
            "query": upsert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    return payload_db



def db_fase_3_a_b_trazabilidad_mediciones(parents_relation):

    # Convertirlos a formato ('', '', '') para postgress
    global_ids_tupla = tuple(parents_relation)
    
    # 1. Va a la base de datos fase 3 a data
    # Busca los circuitos, tipo de punto y fecha fase 3

   
    # Va a la base de datos puntos capa principal
    # Busca cuantos ["TIPO_PUNTO"] de [CIRCUITO_ACU] tienen [HABILITADO_FASE3] y asigna esta 
    # cuenta en una nueva variable
    
    query_sql = f"""
        SELECT 
            f3."CIRCUITO_ACU",
            f3."TIPO_PUNTO",
            f3."FECHA_FASE3",
            COUNT(pcp.*) AS habilitados_fase3
        FROM fase_3_a_data f3
        LEFT JOIN puntos_capa_principal pcp
            ON pcp."CIRCUITO_ACU" = f3."CIRCUITO_ACU"
           AND pcp."TIPO_PUNTO" = f3."TIPO_PUNTO"
           AND pcp."HABILITADO_FASE3" = 1
        WHERE f3."PARENT_ID" IN {global_ids_tupla}
        GROUP BY 
            f3."CIRCUITO_ACU",
            f3."TIPO_PUNTO",
            f3."FECHA_FASE3";
    """

    print("Query fase 3, capa principal", query_sql)
    
    payload_f3_db = {
        "queryStringParameters": {
            "query": query_sql,
            "db_name": "parametros",
            "time_column": "FECHA_FASE3",
        }
    }
    
    filas = invoke_lambda_db(payload_f3_db, DB_ACCESS_LAMBDA_ARN)  # ← 1ra llamada
    print("Filas de 1er llamado:", filas)

    # 2. Obtener trazabilidad existente
    # Va a la base de datos de fase_3_a_b_trazabilidad_mediciones (Unique Id : Circuito)
    # Del payload recibido:

    pares = [(f["CIRCUITO_ACU"], f["TIPO_PUNTO"]) for f in filas]
    pares_tupla = tuple(pares)

    query_trazabilidad = f"""
        SELECT *
        FROM fase_3_a_b_trazabilidad_mediciones
        WHERE ("CIRCUITO_ACU", "TIPO_PUNTO") IN {pares_tupla};
    """
    payload_traza_db = {
        "queryStringParameters": {
            "query": query_trazabilidad,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }

    trazabilidad = invoke_lambda_db(payload_traza_db, DB_ACCESS_LAMBDA_ARN)  # ← 2da llamada

    trazabilidad_dict = {
        (t["CIRCUITO_ACU"], t["TIPO_PUNTO"]): t
        for t in trazabilidad
    }
   
    # 4. PROCESAR CADA FILA
   
    resultados = []
    upserts = []   # ← Aquí guardamos todos los UPSERT SQL

    for fila in filas:

        circuito = fila["CIRCUITO_ACU"]
        tipo_punto = fila["TIPO_PUNTO"]
        fecha_fase3 = fila["FECHA_FASE3"]
        habilitados = fila["habilitados_fase3"]

        #if isinstance(fecha_fase3, str):
            #fecha_fase3 = datetime.fromisoformat(fecha_fase3)

        # Campo que cambia por tipo de punto
        if tipo_punto == "vrp":
            campo_numero = "NUMERO_VRP"
        elif tipo_punto == "puntos_medicion":
            campo_numero = "NUMERO_PUNTOS"
        else:
            campo_numero = None


        # Registro existente o nuevo
        registro = trazabilidad_dict.get((circuito, tipo_punto))
        numero_actual = 0
        if registro and registro.get(campo_numero):
            numero_actual = registro[campo_numero]


        # Plantilla
        resultado = {
            "CIRCUITO_ACU": circuito,
            "TIPO_PUNTO": tipo_punto,
            campo_numero: None,
            "FECHA_INICIO_INSTALACION": None,
            "FECHA_FIN_INSTALACION": None,
            "FECHA_FIN_MEDICION": None
        }

        # 
        # 5. LÓGICA para llenar las columnas de fase_3_a_b_trazabilidad_mediciones
       
        if not numero_actual or numero_actual == 0:
            resultado[campo_numero] = 1
            resultado["FECHA_INICIO_INSTALACION"] = fecha_fase3

        elif numero_actual < habilitados:
            resultado[campo_numero] = numero_actual + 1

        elif numero_actual == habilitados:
            resultado[campo_numero] = numero_actual
            resultado["FECHA_FIN_INSTALACION"] = fecha_fase3

            dia_siguiente = (fecha_fase3 + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            resultado["FECHA_FIN_MEDICION"] = dia_siguiente + timedelta(days=7)

        resultados.append(resultado)

        print("RESULTADOS: ", resultados)
      
        # 6. Construir el UPSERT SQL por cada fila
        

        # Preparar valores SQL
        def sql_value(v):
            if v is None:
                return "NULL"
            if isinstance(v, datetime):
                return f"'{v.isoformat()}'"
            return f"'{v}'"

        numero_val = resultado[campo_numero]

        upsert_sql = f"""
            INSERT INTO fase_3_a_b_trazabilidad_mediciones (
                "CIRCUITO_ACU",
                "TIPO_PUNTO",
                "{campo_numero}",
                "FECHA_INICIO_INSTALACION",
                "FECHA_FIN_INSTALACION",
                "FECHA_FIN_MEDICION"
            )
            VALUES (
                '{circuito}',
                '{tipo_punto}',
                {numero_val},
                {sql_value(resultado["FECHA_INICIO_INSTALACION"])},
                {sql_value(resultado["FECHA_FIN_INSTALACION"])},
                {sql_value(resultado["FECHA_FIN_MEDICION"])}
            )
            ON CONFLICT ("CIRCUITO_ACU", "TIPO_PUNTO")
            DO UPDATE SET 
                "{campo_numero}" = EXCLUDED."{campo_numero}",
                "FECHA_INICIO_INSTALACION" = EXCLUDED."FECHA_INICIO_INSTALACION",
                "FECHA_FIN_INSTALACION" = EXCLUDED."FECHA_FIN_INSTALACION",
                "FECHA_FIN_MEDICION" = EXCLUDED."FECHA_FIN_MEDICION";
        """

        upserts.append(upsert_sql)

    # =====================================================
    # 5. Ejecutar TODO en UNA SOLA llamada a la lambda
    # =====================================================

    upserts_combined = "\n".join(upserts)
    print("UPSERT: ", upserts)
    payload_upsert_db = {
        "queryStringParameters": {
            "query": upserts_combined,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }
    invoke_lambda_db(payload_upsert_db, DB_ACCESS_LAMBDA_ARN)  # ← 3ra llamada total
    return resultados

    
    
    
   
    
   
     # toma el circuito y lo agrega en ["CIRCUITO_ACU"]
     # si ["TIPO_PUNTO"] es vrp --> busca  ["NUMERO_VRP"]
        #Si ["NUMERO_VRP"] == 0 o vacío se asigna [FECHA_INICIO_INSTALACION] = ["FECHA_FASE3"]
        #Si ["NUMERO_VRP"] < "habilitados_fase3" --->  ["NUMERO_VRP"]+= 1
        #Si ["NUMERO_VRP"] == "habilitados_fase3" ---> ["FECHA_FIN_INSTALACION"] = ["FECHA_FASE3"]
            # Se define [FECHA_FIN_MEDICION] = 00 del día después de ["FECHA_FIN_INSTALACION"]
            # Se define [FECHA_FIN_MEDICION] = 7 dias después de [FECHA_FIN_MEDICION]
     # si ["TIPO_PUNTO"] es puntos_medicion --> busca  ["NUMERO_PUNTOS"]
        #Si ["NUMERO_VRP"] == 0 o vacío se asigna [FECHA_INICIO_INSTALACION] = ["FECHA_FASE3"]
        #Si ["NUMERO_VRP"] < "habilitados_fase3" --->  ["NUMERO_VRP"]+= 1
        #Si ["NUMERO_VRP"] == "habilitados_fase3" ---> ["FECHA_FIN_INSTALACION"] = ["FECHA_FASE3"]
            # Se define [FECHA_FIN_MEDICION] = 00 del día después de ["FECHA_FIN_INSTALACION"]
            # Se define [FECHA_FIN_MEDICION] = 7 dias después de [FECHA_FIN_MEDICION]
     
            
            
            