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
   "OBJECTID": "1327",
   "geometry": "null",
      "attributes" : {
         "OBJECTID" : 1, 
         "relation_id" : "aaaaaaaa-1111-bbbb-2222-000000000000", 
         "usuario_creador_fase1" : "null", 
         "fecha_creacion_fase1" : 1760456847, 
         "usuario_ultima_edicion_fase1" : "USER", 
         "fecha_ultima_edicion_fase1" : 1760456847, 
         "tipo_punto_1" : "puntos_medicion", 
         "id" : "M0000", 
         "signos_desgaste" : "No", 
         "fugas" : "No", 
         "daños" : "No", 
         "requiere_instalacion_tapa" : "Si", 
         "requiere_limpieza_de_epm" : "No", 
         "motivo_limpieza" : " ", 
         "requiere_clausura" : "No", 
         "comentario_condicion_fisica_acu" : "condicion fisica ok excepto tapa", 
         "pernos_instalados" : " ", 
         "estado_aseguramiento_tapa" : " ", 
         "estructura_equipos_medicion" : " ", 
         "comentario_condicion_fisica_alc" : " ", 
         "estado_conexion" : "Si", 
         "estado_tuberia" : "Si", 
         "accesorios_existentes" : " ", 
         "valvula_abre" : "Si", 
         "valvula_cierra" : "Si", 
         "flujo_agua" : "Si", 
         "comentario_conexiones_hid" : "Conexion hidraulica ok", 
         "ubicacion_geografica_critica" : "No", 
         "posible_exposicion_al_fraude" : "No", 
         "comentario_vulnerabilidades" : "Sin posibles vulnerabilidades", 
         "verificacion_señal_4g" : "Si", 
         "operador_con_cobertura" : "Claro", 
         "comentario_cobertura" : "Buena cobertura", 
         "requiere_fase1" : "Si", 
         "GlobalID" : "fb6f6bf9-84fb-488e-b84b-0c8dd82b8ad3", 
         "tipo_de_punto" : "puntos_medicion", 
         "equipos_utilizados" : " ", 
         "conclusiones" : "Instalar tapa", 
         "recomendaciones" : "Tapa estandar", 
         "habiltado_fase2" : "No", 
         "habilitado_fase3" : "No", 
         "comentario_general_fase1" : "Instalar tapa en fase 2, previo a fase 3", 
         "actualizacion_ubicacion" :"No"
         },
   "point_type": "puntos_medicion"
   },
   "attachments":[
      "files/temp-image-folder/ejemplo1.jpg",
      "files/temp-image-folder/ejemplo2.jpg",
      "files/temp-image-folder/ejemplo3.jpg",
      "files/temp-image-folder/ejemplo4.jpg",
      "files/temp-image-folder/ejemplo5.jpg",
      "files/temp-image-folder/ejemplo6.jpg",
      "files/temp-image-folder/ejemplo7.jpg",
      "files/temp-image-folder/ejemplo8.jpg"
   ]
}  # valor de prueba

def lambda_handler(event, context):

    incoming_payload = event
    #circuito = event.payload["circuito"] #obtener el circuito del evento
    #incoming_payload = payload_prueba
    id = incoming_payload["payload"]["attributes"]["id"] #id epm
    GlobalID = incoming_payload["payload"]["attributes"]["relation_id"] #id uuid global  relation_id de fase 1 = GlobalID capa principal
    
    # invocar a lambda de generación de formato (async)
    invoke_lambda(incoming_payload, formato_ARN)
    print("Invocada lambda formato")

    # invocar a lambda acceso base de datos (sync) 
    # revisar si el punto es el ultimo visitado del circuito
    payload_db = {
        "queryStringParameters": {
            "query": f"""SELECT CASE WHEN COUNT(*) = (SELECT COUNT(*)  FROM puntos_capa_principal p2  WHERE p2."CIRCUITO_1" = p1."CIRCUITO_1" AND p2.id IN (SELECT id FROM fase_1))THEN 'Finalizado' ELSE 'Incompleto' END AS estado, p1."CIRCUITO_1" as "CIRCUITO_1" FROM puntos_capa_principal p1 WHERE p1."CIRCUITO_1" = ( SELECT "CIRCUITO_1" FROM puntos_capa_principal WHERE "GlobalID"  = '{GlobalID}')GROUP BY p1."CIRCUITO_1";""",
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
    circuito = body[0]["CIRCUITO_1"]

    print(estado)
    print(circuito)

    # Si es ultimo punto, invocar a lambda de generación de informe (async)
    if estado == "Finalizado":
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