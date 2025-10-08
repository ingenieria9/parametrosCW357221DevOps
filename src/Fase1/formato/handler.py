import boto3
import json
import os

client = boto3.client("lambda", region_name="us-east-1") 
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

def lambda_handler(event, context):
    
    print(event)
    '''payload = {
        "queryStringParameters": {
            "query": "INSERT INTO puntos_capa_principal (id, tipo_punto,fecha_creacion) values ('0004', 'caja_medicion', '2024-08-29 10:30:26.000' )",
            "time_column": "fecha_creacion",
            "db_name": "parametros"
        }
    }'''

    #FunctionName = db_access_arn
    #response = invoke_lambda(payload, FunctionName)
    #return response


def invoke_lambda(payload, FunctionName):
    response = client.invoke(
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