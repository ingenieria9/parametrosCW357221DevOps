from urllib import response
import boto3
import json
import os

client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]

json_test =  '''{
  "features" : [
    {
      "attributes" : {
        "OBJECTID" : 32, 
        "FID_ELEM" : 71115, 
        "PUNTO_EXISTENTE" : "Si", 
        "TIPO_PUNTO" : "vrp", 
        "FASE_INICIAL" : "fase1", 
        "VARIABLE_A_MEDIR" : "caudal", 
        "CODIGO_CAJA_ACU" : null, 
        "SUBCIRCUIT_ACU" : 16001, 
        "CUENCA_ALC" : null, 
        "CIRCUITO_ACU" : "ITAGUI", 
        "DIRECCION_ACU" : "CL48 CL47C", 
        "OBSERV_ACU" : "VRP, DIAGNOSTICAR", 
        "CRITERIO_ACU" : "MEDICION DE CAUDAL", 
        "MUNICIPIO_ACU" : "ITAGUI", 
        "TIPO_ELEM_ACU" : "VRP", 
        "IPID_ELEM_ACU" : 4659881, 
        "IPID_TUB_ACU" : 10217263, 
        "DIAME_mm_ACU" : 600, 
        "MATERIAL_ACU" : "ACERO", 
        "x" : -75.6120618000001, 
        "y" : 6.17799329999999, 
        "TIPO_RED_ALC" : null, 
        "PROPIETARI_ALC" : null, 
        "TIPO_AGUA_ALC" : null, 
        "IPID_ALC" : null, 
        "ETAPA_ALC" : null, 
        "FECHA_INI_ALC" : null, 
        "FECHA_FIN_ALC" : null, 
        "QPROM_ALC" : null, 
        "QMAX_ALC" : null, 
        "QMIN_ALC" : null, 
        "DIAMETRO_ALC" : null, 
        "PENDIENTE_ALC" : null, 
        "EST_SIATA_ALC" : null, 
        "X_m" : 830156.226295278, 
        "Y_m" : 1175151.96232507, 
        "GlobalID" : "07550520-ec9c-462e-9086-c66bfccf2cf3", 
        "USUARIO_CREACION" : null, 
        "FECHA_CREACION" : null, 
        "USUARIO_EDICION" : null, 
        "FECHA_EDICION" : null
      }, 
      "geometry" : 
      {
        "x" : -75.612061799999935, 
        "y" : 6.1779933000000256
      }
    }    
  ]
}'''


from datetime import datetime, timezone


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

def lambda_handler(event, context):
    batch_size = 10
    puntos_capa_principal_fields = ["GlobalID","TIPO_PUNTO","FECHA_CREACION","FECHA_EDICION","CIRCUITO_ACU","SUBCIRCUITO_ACU",
                                    "CUENCA_ALC","FID_ELEM","DIRECCION_ACU","CODIGO_CAJA_ACU","x","y","PUNTO_EXISTENTE","IPID_ELEM_ACU",
                                    "IPID_ALC","FASE_INICIAL","VARIABLE_A_MEDIR"]

    insert_values = []

    json_data = json.loads(json_test)

    for feature in json_data.get("features", []):
        atributos = feature.get("attributes", {})
        # Construir fila SQL (solo los campos de la lista)
        values = []
        for field in puntos_capa_principal_fields:
            val = atributos.get(field)
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

    # --- Construir el INSERT ---
    insert_sql = f"""
        INSERT INTO puntos_capa_principal ({', '.join(f'"{f}"' for f in puntos_capa_principal_fields)})
        VALUES {', '.join(insert_values)};
    """

    print("insert_sql", insert_sql)
    print("values", values)

    # --- Crear payload para Lambda ---
    payload_db = {
        "queryStringParameters": {
            "query": insert_sql,
            "db_name": "parametros",
            "time_column": "FECHA_CREACION",
        }
    }

    #print(payload_db)

    # Invocar la lambda con el lote
    invoke_lambda_db(payload_db, DB_ACCESS_LAMBDA_ARN)


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