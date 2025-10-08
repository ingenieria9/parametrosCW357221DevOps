import boto3
import json
import os

client = boto3.client("lambda", region_name="us-east-1") 

def lambda_handler(event, context):
    
    db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

    response = client.invoke(
        FunctionName=db_access_arn,  # ARN completo
        InvocationType="RequestResponse",  # o 'Event' si no quieres esperar respuesta
        Payload=json.dumps({
            {
            "queryStringParameters": {
                "query": "SELECT * FROM puntos_capa_principal",
                "time_column": "timestamp",
                "db_name": "parametros"
            }
            }
        })
    )
    result = json.load(response)
    return result
