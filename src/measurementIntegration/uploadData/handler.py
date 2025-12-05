import boto3
import os
import json
import csv
import io

s3 = boto3.client('s3')
client_lambda_db = boto3.client("lambda", region_name="us-east-1") 

db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

def lambda_handler(event, context):
    print("Event recibido:", json.dumps(event))
    
    # Obtener información del evento S3
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    print(f"Procesando archivo: s3://{bucket}/{key}")

    # Extraer IDENTIFICADOR_DATALOGGER del nombre de archivo antes del "_"
    file_name = key.split("/")[-1]
    id_datalogger = file_name.split("_")[0]
    print("IDENTIFICADOR_DATALOGGER:", id_datalogger)

    # Descargar el CSV desde S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    csv_data = list(csv.reader(io.StringIO(content)))

    # Obtener FID_ELEM y TIPO_PUNTO desde la DB
    query_status = f"""
        SELECT "FID_ELEM", "TIPO_PUNTO", "CIRCUITO_ACU"
        FROM fase_3_a_status 
        WHERE "IDENTIFICADOR_DATALOGGER" = '{id_datalogger}';
    """

    payload_status = {
        "queryStringParameters": {
            "query": query_status,
            "db_name": "parametros"
        }
    }

    

    status_result = invoke_lambda_db(payload_status, db_access_arn)
    print("Resultado status:", status_result)

    if not status_result or not status_result.get("body"):
        print("No se encontraron datos en fase_3_a_status para", id_datalogger)
        return {"statusCode": 404, "body": f"No data for {id_datalogger}"}

    try:
        status_body = json.loads(status_result["body"])
        fid_elem = status_body[0]["FID_ELEM"]
        tipo_punto = status_body[0]["TIPO_PUNTO"]
        circuito_acu = status_body[0]["CIRCUITO_ACU"]
    except Exception as e:
        print("Error parseando respuesta de status:", e)
        return {"statusCode": 500, "body": "Invalid response from dbAccess"}

    # Procesar CSV desde la fila 21 (índice 20)
    data_rows = csv_data[20:]
    print(f"Total filas de datos: {len(data_rows)}")

    registros = []
    for row in data_rows:
        if len(row) < 3 or not row[0].strip():
            continue  # saltar filas vacías

        fecha_hora = row[0].strip()
        presion = row[1].strip() if row[1] is not None else ""
        temperatura = row[2].strip() if row[2] is not None else ""

        presion_sql = presion.replace(',', '.') if presion else None
        temperatura_sql = temperatura.replace(',', '.') if temperatura else None

        presion_val = presion_sql if presion_sql is not None else "NULL"
        temperatura_val = temperatura_sql if temperatura_sql is not None else "NULL"

        registros.append(
            f"('{fecha_hora}', {presion_val}, {temperatura_val}, "
            f"'{id_datalogger}', '{fid_elem}', '{tipo_punto}', '{circuito_acu}')"
        )

    # Armar e insertar por lotes de 500 registros
    batch_size = 500
    for i in range(0, len(registros), batch_size):
        batch = registros[i:i+batch_size]
        values_sql = ",\n".join(batch)
        print("values sql:",values_sql)
        insert_query = f"""
            INSERT INTO fase_3_b_mediciones 
            ("FECHA_HORA", "PRESION_1", "TEMPERATURA", "IDENTIFICADOR_DATALOGGER", "FID_ELEM", "TIPO_PUNTO", "CIRCUITO_ACU")
            VALUES {values_sql};
        """

        payload_insert = {
            "queryStringParameters": {
                "query": insert_query,
                "db_name": "parametros",
                "time_column": "FECHA_HORA"
            }
        }

 

        print(f"Insertando batch {i//batch_size + 1} con {len(batch)} registros...")
        insert_result = invoke_lambda_db(payload_insert, db_access_arn)
        print("Respuesta insert:", insert_result)

    print("Procesamiento completo")
    return {"statusCode": 200, "body": "OK"}


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
