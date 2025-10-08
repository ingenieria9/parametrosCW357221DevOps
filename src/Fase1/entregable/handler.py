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

    #incoming_payload = event.payload
    #circuito = event.payload["circuito"]
    incoming_payload = {"test"  : "data"}  # valor de prueba
    circuito = 'tmk'  # valor de prueba
    
    # invocar a lambda de generación de formato (async)
    invoke_lambda(incoming_payload, formato_ARN)
    print("Invocada lambda formato")

    # invocar a lambda acceso base de datos (sync) 
    # revisar si el punto es el ultimo visitado del circuito
    payload_db = {
        "queryStringParameters": {
            # seleccionar circuito de la tabla puntos_capa_principal si todos los id de tabla puntos_capa_principal estan en tabla fase_1
            "query": """SELECT 
                CASE 
                    WHEN COUNT(*) = (
                        SELECT COUNT(*) 
                        FROM puntos_capa_principal p2
                        WHERE p2.circuito = p1.circuito
                        AND p2.id IN (SELECT id FROM fase_1)
                    ) THEN 'Finalizado'
                    ELSE 'Incompleto'
                END AS estado
            FROM puntos_capa_principal p1 WHERE p1.circuito = 'tmk' GROUP BY p1.circuito;
            """,
            "time_column": "fecha_creacion",
            "db_name": "parametros"
        }
    }
    response_db =invoke_lambda_db(payload_db, db_access_arn)
    #Parsear el body 
    body = json.loads(response_db["body"])
    # xtraer el valor del campo "estado"
    estado = body[0]["estado"]

    print(estado)


    # Si es ultimo punto, invocar a lambda de generación de informe (async)
    if estado == "Finalizado":
        payload_informe = {
            "circuito": circuito
        }
        invoke_lambda(payload_informe, informe_ARN)
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