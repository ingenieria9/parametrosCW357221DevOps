from urllib import response
import boto3
import json
import os

client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]

json_test =  ''' {
    "features" : [
      "attributes" : {
        "OBJECTID" : 1, 
        "FID_ELEM" : 15257, 
        "PUNTO_EXISTENTE" : "No", 
        "TIPO_PUNTO" : "puntos_medicion", 
        "FASE_INICIAL" : "fase2", 
        "VARIABLE_A_MEDIR" : "presion", 
        "CODIGO_CAJA_ACU" : null, 
        "SUBCIRCUIT_ACU" : 16002, 
        "CUENCA_ALC" : null, 
        "CIRCUITO_ACU" : "ITAGUI", 
        "DIRECCION_ACU" : "CR 52 CL 44 -3", 
        "OBSERV_ACU" : "PUNTO DE MEDICION PROYECTADO", 
        "CRITERIO_ACU" : "MEDICION DE PRESION", 
        "MUNICIPIO_ACU" : "ITAGUI", 
        "TIPO_ELEM_ACU" : "NODO", 
        "IPID_ELEM_ACU" : 2069372, 
        "IPID_TUB_ACU" : 2069443, 
        "DIAME_mm_ACU" : 75, 
        "MATERIAL_ACU" : "PVC", 
        "x" : -75.614342552, 
        "y" : 6.17164153200002, 
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
        "X_m" : 829903.6738794, 
        "Y_m" : 1174449.39284835, 
        "GlobalID" : "0bfc161b-9eec-440c-8a34-fdd34b22df5b", 
        "USUARIO_CREACION" : null, 
        "FECHA_CREACION" : null, 
        "USUARIO_EDICION" : "USER", 
        "FECHA_EDICION" : 1761315126000
      }, 
      "geometry" : 
      {
        "x" : -75.614342551999982, 
        "y" : 6.1716415320000237
      }
    }, 
    {
      "attributes" : {
        "OBJECTID" : 2, 
        "FID_ELEM" : 15450, 
        "PUNTO_EXISTENTE" : "No", 
        "TIPO_PUNTO" : "puntos_medicion", 
        "FASE_INICIAL" : "fase2", 
        "VARIABLE_A_MEDIR" : "presion", 
        "CODIGO_CAJA_ACU" : null, 
        "SUBCIRCUIT_ACU" : 16005, 
        "CUENCA_ALC" : null, 
        "CIRCUITO_ACU" : "ITAGUI", 
        "DIRECCION_ACU" : "CR 56D CL 44", 
        "OBSERV_ACU" : "PUNTO DE MEDICION PROYECTADO", 
        "CRITERIO_ACU" : "MEDICION DE PRESION", 
        "MUNICIPIO_ACU" : "ITAGUI", 
        "TIPO_ELEM_ACU" : "NODO", 
        "IPID_ELEM_ACU" : 4614371, 
        "IPID_TUB_ACU" : 4614372, 
        "DIAME_mm_ACU" : 75, 
        "MATERIAL_ACU" : "PVC", 
        "x" : -75.616507407, 
        "y" : 6.17470936300002, 
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
        "X_m" : 829664.046916928, 
        "Y_m" : 1174788.75993623, 
        "GlobalID" : "1497f238-9e64-4fc2-94b9-ae35c39aa020", 
        "USUARIO_CREACION" : null, 
        "FECHA_CREACION" : null, 
        "USUARIO_EDICION" : null, 
        "FECHA_EDICION" : null
      }, 
      "geometry" : 
      {
        "x" : -75.616507406999972, 
        "y" : 6.1747093630000336
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