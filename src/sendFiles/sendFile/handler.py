import requests
import boto3
import json
import os
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, date, timedelta, timezone
import smtplib
from pathlib import Path

client_lambda_db = boto3.client("lambda", region_name="us-east-1")
db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]
bucket_name = os.environ['BUCKET_NAME']

TMP_DIR = Path("/tmp")
s3_client = boto3.client('s3')
s3_bucket = os.environ['BUCKET_NAME']


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


def send_email(project_name, s3_keys, s3_client, s3_bucket, today_date_string, subject_override, recipient_email):
    #recipient_email = "natalia.tamayo@telemetrik.com.co,mateo.carmona@telemetrik.com.co,valentina.lopez@telemetrik.com.co"
    subject = subject_override

    sender_email = 'alarmas@telemetrik.com.co'
    app_password = os.environ['GMAIL_EMAIL_PASSWORD']

    # --- Generar URLs firmadas ---
    signed_urls = []
    for s3_key in s3_keys:
        try:
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': s3_bucket, 'Key': s3_key},
                ExpiresIn=432000  # 5 días
            )
            signed_urls.append((os.path.basename(s3_key), url))
        except Exception as e:
            print(f"Error generando URL firmada para {s3_key}: {e}")

    # --- Agrupar por circuito ---
    circuit_pattern = r"MPH-EJ-0601-(\w+)-F01-ACU-(?:EIN|DIA)-001\.(xlsx|docx)"
    circuits = {}

    for filename, url in signed_urls:
        match = re.search(circuit_pattern, filename)
        if match:
            cod = match.group(1)
            ext = match.group(2)
            circuits.setdefault(cod, {}).setdefault(ext, url)

    # --- Cuerpo del correo (HTML) ---
    now_utc5 = datetime.utcnow() - timedelta(hours=5)
    timestamp = now_utc5.strftime("%Y-%m-%d_%H-%M")

    body_html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <p>¡Hola!</p>
        <p>
          Adjuntamos los entregables generados para el proyecto <b>{project_name}</b> correspondientes al día <b>{today_date_string}</b>.
        </p>
        <p>
          Revisar el archivo tipo informe (<b>.docx</b>) para ajustar comentarios e imágenes y luego exportar a PDF.
          Tambien revisar el archivo tipo formato para ajustar cambios.
        </p>
        <p><b>El link de descarga esta disponibles por 5 días:</b></p>
        <ul>
    """

    for cod, files in circuits.items():
        body_html += f"<li><b>{cod}</b>: "
        parts = []
        if "xlsx" in files:
            parts.append(
                f'Formato: <a href="{files["xlsx"]}">formato_{timestamp}.xlsx</a>'
            )
        if "docx" in files:
            parts.append(
                f'Informe: <a href="{files["docx"]}">informe_{timestamp}.docx</a>'
            )
        body_html += " y ".join(parts) + "</li>"

    body_html += """
        </ul>
        <p>Saludos,</p>
      </body>
    </html>
    """

    # --- Construir mensaje ---
    msg = MIMEMultipart('alternative')
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))

    # --- Enviar correo ---
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, app_password)
        recipient_list = [email.strip() for email in recipient_email.split(',')]
        server.sendmail(sender_email, recipient_list, msg.as_string())
        server.close()

        print("Correo enviado correctamente con links firmados.")
        return {'statusCode': 200, 'body': 'Email sent successfully!'}

    except Exception as e:
        print("Error enviando email:", e)
        return {'statusCode': 500, 'body': f'Failed to send email. Error: {str(e)}'}


def lambda_handler(event, context):
    print("Evento recibido:", json.dumps(event))

    # --- Determinar origen ---
    if "body" in event:
        print("Ejecución manual desde API Gateway.")
        try:
            body = json.loads(event["body"])
        except Exception as e:
            return {"statusCode": 400, "body": f"Error al leer el body: {str(e)}"}

        fecha_str = body.get("fecha")
        circuito_cuenca_valor = body.get("circuito")
        ACU = str(body.get("ACU", "true")).lower() == "true"
        correo = body.get("correo", "natalia.tamayo@telemetrik.com.co,mateo.carmona@telemetrik.com.co,valentina.lopez@telemetrik.com.co")

    else:
        print("Ejecución programada diaria.")
        fecha_str = None
        circuito_cuenca_valor = None
        ACU = True

    # --- Determinar fecha con timezone UTC-5 ---
    tz = timezone(timedelta(hours=-5))

    if fecha_str:
        target_date = datetime.strptime(fecha_str, "%Y-%m-%d").date()
    else:
        target_date = datetime.now(tz).date()

    today_date_string = target_date.strftime('%Y-%m-%d')

    # --- Prefijo base ---
    prefix_base = "files/entregables/Fase1/ACU/CIR/" if ACU else "files/entregables/Fase1/ALC/CIR/"
    paginator = s3_client.get_paginator('list_objects_v2')
    all_files = []

    # --- Caso circuito ---
    if circuito_cuenca_valor:
        tmp_path_code = TMP_DIR / "code.json"
        code_file = "files/epm_codes/CODE_ACU_CIR.json" if ACU else "files/epm_codes/CODE_ALC_CUE.json"

        print(f"Descargando archivo de códigos: {code_file}")
        s3_client.download_file(bucket_name, code_file, str(tmp_path_code))

        with open(tmp_path_code, "r", encoding="utf-8") as f:
            contenido = f.read().strip()
            if not contenido:
                print("El archivo JSON está vacío.")
                return {"statusCode": 500, "body": "Archivo de códigos vacío."}

            try:
                code_json = json.loads(contenido)
            except json.JSONDecodeError as e:
                print(f"Error al parsear JSON {code_file}: {e}")
                return {"statusCode": 500, "body": "Error al parsear el JSON de códigos."}

        cod = code_json.get(circuito_cuenca_valor, circuito_cuenca_valor)

        print(f"Buscando archivos en {prefix_base} que contengan el código {cod}...")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_base):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith(('.xlsx', '.docx')) and cod in key:
                    all_files.append(key)

        print(f"Archivos filtrados por circuito {circuito_cuenca_valor} (COD: {cod}): {len(all_files)}")

    # --- Caso por fecha ---
    else:
        print(f"Buscando archivos del {target_date} en {prefix_base}...")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_base):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith(('.xlsx', '.docx')):
                    last_modified = obj["LastModified"].astimezone(tz)
                    if last_modified.date() == target_date:
                        all_files.append(key)

        print(f"Archivos encontrados para la fecha {target_date}: {len(all_files)}")

    # --- Agrupar y enviar email ---
    circuit_pattern = r"MPH-EJ-0601-(\w+)-F01-ACU-(?:EIN|DIA)-001\.(?:xlsx|docx)"
    circuits = {}

    for key in all_files:
        match = re.search(circuit_pattern, key)
        if match:
            cod = match.group(1)
            circuits.setdefault(cod, []).append(key)

    finalized_circuits = {cod: files for cod, files in circuits.items() if len(files) >= 1}

    for cod, s3_keys in finalized_circuits.items():
        subject_circuit = f"CW357221-ACU-CIR-{cod} - Entregables {today_date_string}"
        print(f"Enviando correo para circuito {cod}: {s3_keys}")
        send_email("CW357221-MPH", s3_keys, s3_client, bucket_name, today_date_string, subject_circuit, correo)

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "ok", "archivos_encontrados": len(all_files)}),
        "headers": {"Content-Type": "application/json"}
    }