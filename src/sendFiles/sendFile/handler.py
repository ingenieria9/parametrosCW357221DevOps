import requests
import boto3
import json
import os
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from email.mime.base import MIMEBase
from email import encoders
from datetime import date, timedelta, datetime
import smtplib

client_lambda_db = boto3.client("lambda", region_name="us-east-1") 
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]
bucket_name = os.environ['BUCKET_NAME']

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

import os
import boto3
import smtplib
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_email(project_name, s3_keys, s3_client, s3_bucket, today_date_string):
    """Envía un correo con los archivos indicados en s3_keys como adjuntos."""
    recipient_email = "natalia.tamayo@telemetrik.com.co"

    subject = f" {project_name} Envio diario entregables"
    body = (
        f"¡Hola!\n\n"
        f"Adjunto encontrarás los entregables generados para el proyecto {project_name} correspondientes al día {today_date_string}.\n\n"
        f"Recordar revisar y ajustar documento por circuito si aplica."
    )

    sender_email = 'alarmas@telemetrik.com.co'
    app_password = os.environ['GMAIL_EMAIL_PASSWORD']

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Adjuntar cada archivo de S3
    for s3_key in s3_keys:
        try:
            s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            attachment_data = s3_response['Body'].read()

            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment_data)
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename={os.path.basename(s3_key)}',
            )
            msg.attach(part)
            print(f"Adjuntado: {s3_key}")
        except Exception as e:
            print(f"Error al adjuntar {s3_key}: {e}")

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, app_password)
        server.sendmail(sender_email, recipient_email.split(','), msg.as_string())
        server.close()
        print("Correo enviado correctamente.")
        return {'statusCode': 200, 'body': 'Email sent successfully!'}
    except Exception as e:
        print("Error enviando email:", e)
        return {'statusCode': 500, 'body': f'Failed to send email. Error: {str(e)}'}



def lambda_handler(event, context):
    """Busca archivos en S3 y los envía por correo según las condiciones definidas."""
    s3_client = boto3.client('s3')
    s3_bucket = os.environ['BUCKET_NAME']
    db_access_arn = os.environ['DB_ACCESS_LAMBDA_ARN']

    # Fecha actual (zona horaria -5)
    today_date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-5)))
    today_date_string = today_date.strftime('%Y-%m-%d')

    #  Consultar DB para obtener los FID_ELEM del día
    payload_db = {
        "queryStringParameters": { 
            "query": f"""SELECT "FID_ELEM" FROM fase_1 WHERE DATE("FECHA_FASE1") = '{today_date_string}';""",
            "time_column": "FECHA_FASE1",
            "db_name": "parametros"
        }
    }

    response_db = invoke_lambda_db(payload_db, db_access_arn)
    body = json.loads(response_db["body"])
    FID_ELEMs = [item["FID_ELEM"] for item in body if "FID_ELEM" in item]

    if not FID_ELEMs:
        print("No se encontraron FID_ELEM para hoy.")
        return {'statusCode': 200, 'body': 'No se encontraron entregables para hoy.'}

    print(f"FID_ELEM encontrados: {FID_ELEMs}")

    #  Buscar archivos en S3
    s3_files_to_send = []

    # Prefijos a buscar
    prefixes = [
        "files/entregables/Fase1/ACU/PM/",
        "files/entregables/Fase1/ACU/VRP/",
        "files/entregables/Fase1/ACU/CIR/"
    ]

    paginator = s3_client.get_paginator('list_objects_v2')

    for prefix in prefixes:
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']

                # Caso 1: PM y VRP -> solo PDFs que contengan FID_ELEM
                if prefix.endswith(("PM/", "VRP/")):
                    if key.endswith(".pdf") and any(pid in key for pid in FID_ELEMs):
                        s3_files_to_send.append(key)

                # Caso 2: CIR -> archivos modificados hoy
                elif prefix.endswith("CIR/"):
                    last_modified = obj["LastModified"].astimezone(datetime.timezone(datetime.timedelta(hours=-5)))
                    if last_modified.date() == today_date.date():
                        s3_files_to_send.append(key)

    if not s3_files_to_send:
        print("No se encontraron archivos que cumplan las condiciones.")
        return {'statusCode': 200, 'body': 'No hay archivos para enviar.'}

    print(f"Archivos a enviar: {s3_files_to_send}")

    # Enviar los archivos por email
    send_email("CW357221-MPH", s3_files_to_send, s3_client, s3_bucket, today_date_string)

    return {
        'statusCode': 200,
        'body': '{"status": "ok"}',
        'headers': {'Content-Type': 'application/json'}
    }