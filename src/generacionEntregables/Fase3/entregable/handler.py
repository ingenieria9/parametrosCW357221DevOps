from urllib import response
import boto3
import json
import os

#Esta lambda gestiona la orquestación del proceso de generación de entregables para la Fase 1

client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
client_lambda = boto3.client("lambda") 

formato_ARN = os.environ["formato_ARN"]
informe_ARN = os.environ["informe_ARN"]
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

def lambda_handler(event, context):
    print(event)

    incoming_payload = event
    #circuito = event.payload["circuito"] #obtener el circuito del evento
    #incoming_payload = payload_prueba
    FID_ELEM = incoming_payload["payload"]["FID_ELEM"] #FID
    GlobalID = incoming_payload["payload"]["PARENT_ID"] #PARENT_ID de fase 3 = GlobalID capa principal
    CIRCUITO_ACU = incoming_payload["payload"]["CIRCUITO_ACU"] #CIRCUITO_ACU de fase 3
    forzarInforme = incoming_payload.get("forzarInforme", "false")
    
    # invocar a lambda de generación de formato (async)
    invoke_lambda(incoming_payload, formato_ARN)
    print("Invocada lambda formato")

    # invocar a lambda acceso base de datos (sync) 
    # revisar si el punto es el ultimo visitado del circuito

    # TO-DO : revisar query para hacer referencia a fase 3 con tabla trazabilidad (asegurar que finalizado = 1)
    # mirar pertinencia (igual se fuerza desde grafana)
    payload_db = {
        "queryStringParameters": {
            "query": f"""select case when "FINALIZADO" = 1 then 'Finalizado' else 'incompleto' end as "estado"  from trazabilidad_mediciones tm where "CIRCUITO_ACU" = '{CIRCUITO_ACU}'""",
            "time_column": "fecha_creacion",
            "db_name": "parametros"
        }
    }
    print(payload_db)
    response_db =invoke_lambda_db(payload_db, db_access_arn)
    print(response_db)
    #Parsear el body 
    body = json.loads(response_db["body"])
    # Extraer el valor del campo "estado"
    estado = body[0]["estado"]
    #circuito = body[0]["CIRCUITO_ACU"]

    print(estado)
    #print(circuito)

    # Si es ultimo punto, invocar a lambda de generación de informe (async) (O si es forzado por API)
    if estado == "Finalizado" or str(forzarInforme).lower() == "true":
        invoke_lambda(incoming_payload, informe_ARN)
        print("Invocada lambda informe")

    return {
        "statusCode": 200,
        "body": f"Hola Mundo, desde Lambda {context.function_name}!",
    }


def invoke_lambda(payload, FunctionName):
    response = client_lambda.invoke(
        FunctionName=FunctionName,
        InvocationType='Event',  # async
        Payload=json.dumps(payload).encode('utf-8')  #  convierte a JSON y luego a bytes
    )
    return response


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