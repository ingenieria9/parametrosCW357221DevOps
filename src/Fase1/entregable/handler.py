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


payload_prueba = {
    "payload":{
        "layer_id": 1,
        "OBJECTID": '0002',
        "geometry": "null",
        "attributes": {
            "OBJECTID": '0002',
            "GlobalID": "3CFDE950-8AE7-440E-B1E7-310C56A35794",
            "Identificador": "PTO_0002",
            "Tipo_Punto": "VRP",
            "Creador": "central_ti_telemetrik",
            "Fecha_Creacion": 1758818476306,
            "Editor": "central_ti_telemetrik",
            "Fecha_Edicion": 1758829344252,
            "Sí": "Sí",
            "Fugas": "No",
            "Signos_de_desgaste": "null"
        },
        "point_type": "VRP"
        }, 
        "attachments" : ["key1, key2, key3"]
        }  # valor de prueba

def lambda_handler(event, context):

    #incoming_payload = event
    #circuito = event.payload["circuito"] #obtener el circuito del evento
    incoming_payload = payload_prueba
    id = incoming_payload["payload"]["OBJECTID"]
    
    # invocar a lambda de generación de formato (async)
    invoke_lambda(incoming_payload, formato_ARN)
    print("Invocada lambda formato")

    # invocar a lambda acceso base de datos (sync) 
    # revisar si el punto es el ultimo visitado del circuito
    payload_db = {
        "queryStringParameters": {
            "query": f"SELECT CASE WHEN COUNT(*) = (SELECT COUNT(*)  FROM puntos_capa_principal p2  WHERE p2.circuito = p1.circuito AND p2.id IN (SELECT id FROM fase_1))THEN 'Finalizado' ELSE 'Incompleto' END AS estado, p1.circuito as circuito FROM puntos_capa_principal p1 WHERE p1.circuito = ( SELECT circuito FROM puntos_capa_principal WHERE id = '{id}')GROUP BY p1.circuito;",
            "time_column": "fecha_creacion",
            "db_name": "parametros"
        }
    }
    print(payload_db)
    response_db =invoke_lambda_db(payload_db, db_access_arn)
    print(response_db)
    #Parsear el body 
    body = json.loads(response_db["body"])
    # xtraer el valor del campo "estado"
    estado = body[0]["estado"]
    circuito = body[0]["circuito"]

    print(estado)
    print(circuito)

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