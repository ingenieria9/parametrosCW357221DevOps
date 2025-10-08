import boto3
import json
import os

client = boto3.client("lambda", region_name="us-east-1") 
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

#Ejemplo de payload 
#{'payload': {'layer_id': 1, 'OBJECTID': '0002', 'geometry': 'null', 'attributes': {'OBJECTID': '0002', 'GlobalID': '3CFDE950-8AE7-440E-B1E7-310C56A35794', 'Identificador': 'PTO_0002', 'Tipo_Punto': 'VRP', 'Creador': 'central_ti_telemetrik', 'Fecha_Creacion': 1758818476306, 'Editor': 'central_ti_telemetrik', 'Fecha_Edicion': 1758829344252, 'Sí': 'Sí', 'Fugas': 'No', 'Signos_de_desgaste': 'null'}, 'point_type': 'VRP'}, 'attachments': ['CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg']}

def lambda_handler(event, context):
    
    #incoming_payload = event
    incoming_payload = {'payload': {'layer_id': 1, 'OBJECTID': '0002', 'geometry': 'null', 'attributes': {'OBJECTID': '0002', 'GlobalID': '3CFDE950-8AE7-440E-B1E7-310C56A35794', 'Identificador': 'PTO_0002', 'Tipo_Punto': 'VRP', 'Creador': 'central_ti_telemetrik', 'Fecha_Creacion': 1758818476306, 'Editor': 'central_ti_telemetrik', 'Fecha_Edicion': 1758829344252, 'Sí': 'Sí', 'Fugas': 'No', 'Signos_de_desgaste': 'null'}, 'point_type': 'VRP'}, 'attachments': ['CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg, CW357221-ArcGIS-Data/Puntos/1402_VRP/Fase1/attachment_1402_VRP.jpeg']}

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