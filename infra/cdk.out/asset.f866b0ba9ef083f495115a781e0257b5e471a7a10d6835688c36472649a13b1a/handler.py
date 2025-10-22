from urllib import response
import boto3
import json
import os

client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
DB_ACCESS_LAMBDA_ARN = os.environ["DB_ACCESS_LAMBDA_ARN"]

json_test =  ''' {
    "features" : [
    {
      "attributes" : {
        "OBJECTID" : 1, 
        "MUNICIPIO_ACU" : "RIONEGRO", 
        "CIRCUITO_ACU" : "FONTIBON", 
        "TIPO_ELEM_AYA" : "VALVULA SECUNDARIA", 
        "IPID_ELEM_ACU" : "10338841", 
        "FID_ELEM_ACU" : "10338841", 
        "IPID_TUB_ACU" : "10338873", 
        "DIAME_mm_ACU" : 125, 
        "MATERIAL_ACU" : "POLIETILENO ALTA DENSIDAD", 
        "SUBCIRCUITO_ACU" : "29200", 
        "DIRECCION_AYA" : "CL 51 CR 59 A -15 (AP 201 )", 
        "OBSERVACION_ACU" : "PROYECTADA", 
        "CRITERIO_ACU" : "PUNTO ALEJADO DE LA RED", 
        "X_m_ACU" : 855936.173465, 
        "Y_m_ACU" : 1172905.737964, 
        "TIPO_PUNTO_AYA" : "puntos_medicion", 
        "PUNTO_EXISTENTE_AYA" : "Si", 
        "G3E_FID_ALC" : null, 
        "TIPO_RED_ALC" : null, 
        "PROPIETARI_ALC" : null, 
        "TIPO_AGUA_ALC" : null, 
        "CUENCA_ALC" : null, 
        "COOR_X_ALC" : null, 
        "COOR_Y_ALC" : null, 
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
        "IPID_ELEM_AYA" : "10338841", 
        "X_m_AYA" : 855936.173465, 
        "Y_m_AYA" : 1172905.737964, 
        "USUARIO_CREACION" : null, 
        "FECHA_CREACION" : null, 
        "USUARIO_EDICION" : "central_ti_telemetrik", 
        "FECHA_EDICION" : 1760963909642, 
        "GlobalID" : "d1dd9f03-db73-44c5-b9c9-20284ef35818", 
        "FID_AYA" : "10338841", 
        "CODIGO_CAJA" : null
      }, 
      "geometry" : 
      {
        "x" : 855936.1735, 
        "y" : 1172905.738
      }
    }, 
    {
      "attributes" : {
        "OBJECTID" : 2, 
        "MUNICIPIO_ACU" : "RIONEGRO", 
        "CIRCUITO_ACU" : "FONTIBON", 
        "TIPO_ELEM_AYA" : "VALVULA SECUNDARIA", 
        "IPID_ELEM_ACU" : "10226718", 
        "FID_ELEM_ACU" : "10226718", 
        "IPID_TUB_ACU" : "10226744", 
        "DIAME_mm_ACU" : 180, 
        "MATERIAL_ACU" : "POLIETILENO ALTA DENSIDAD", 
        "SUBCIRCUITO_ACU" : "29200", 
        "DIRECCION_AYA" : "CL 67 CR 54 -365", 
        "OBSERVACION_ACU" : "PROYECTADA", 
        "CRITERIO_ACU" : "VERIFICAR FUNCIONAMIENTO DE LA RED", 
        "X_m_ACU" : 855250.609008, 
        "Y_m_ACU" : 1173820.738949, 
        "TIPO_PUNTO_AYA" : "puntos_medicion", 
        "PUNTO_EXISTENTE_AYA" : "Si", 
        "G3E_FID_ALC" : null, 
        "TIPO_RED_ALC" : null, 
        "PROPIETARI_ALC" : null, 
        "TIPO_AGUA_ALC" : null, 
        "CUENCA_ALC" : null, 
        "COOR_X_ALC" : null, 
        "COOR_Y_ALC" : null, 
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
        "IPID_ELEM_AYA" : "10226718", 
        "X_m_AYA" : 855250.609008, 
        "Y_m_AYA" : 1173820.738949, 
        "USUARIO_CREACION" : null, 
        "FECHA_CREACION" : null, 
        "USUARIO_EDICION" : "central_ti_telemetrik", 
        "FECHA_EDICION" : 1760963910442, 
        "GlobalID" : "f42e6e69-e331-4548-a19c-d4c643cd4a23", 
        "FID_AYA" : "10226718", 
        "CODIGO_CAJA" : null
      }, 
      "geometry" : 
      {
        "x" : 855250.609, 
        "y" : 1173820.7389
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
    puntos_capa_principal_fields = ["GlobalID","TIPO_PUNTO_AYA","FECHA_CREACION","FECHA_EDICION","CIRCUITO_ACU",
                                    "SUBCIRCUITO_ACU","CUENCA_ALC","FID_AYA","DIRECCION_AYA","CODIGO_CAJA_ACU",
                                    "X_m_AYA","Y_m_AYA","PUNTO_EXISTENTE_AYA","IPID_ELEM_AYA"]

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