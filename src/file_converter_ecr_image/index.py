# app.py
import os
import boto3
import uuid
import zipfile
import shutil
import subprocess
import logging
from urllib.parse import unquote_plus

import urllib

# Lambda function to convert DOCX to PDF using LibreOffice in an ECR image  
s3 = boto3.client('s3')

bucket = os.environ.get('BUCKET_NAME') # Nombre del bucket S3 desde variable de entorno

def lambda_handler(event, context):
    bucket = bucket
    #key = "output-dev.docx"
    for record in event["Records"]:
        #bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])  # decodifica espacios y caracteres especiales
        
        if not bucket or not key:
            raise ValueError("Necesito bucket y key en el evento")

        uid = str(uuid.uuid4())
        tmpdir = f"/tmp/{uid}"
        os.makedirs(tmpdir, exist_ok=True)
        local_docx = os.path.join(tmpdir, os.path.basename(key))

        try:
            # 1) download
            download_s3(bucket, key, local_docx)

            # 2) extract images from docx
            '''
            images_out = os.path.join(tmpdir, "images")
            images = extract_images_from_docx(local_docx, images_out)
            uploaded_images = []
            for img_path in images:
                img_key = f"converted_images/{os.path.splitext(os.path.basename(key))[0]}/{os.path.basename(img_path)}"
                uploaded_images.append(upload_file(bucket, img_key, img_path))
            '''

            # 3) convert docx -> pdf
            pdf_outdir = os.path.join(tmpdir, "pdf")
            pdf_path = convert_docx_to_pdf(local_docx, pdf_outdir, timeout=240)

            # 4) upload pdf
            pdf_key = f"files/pdf_files/{os.path.splitext(os.path.basename(key))[0]}.pdf"
            pdf_s3 = upload_file(bucket, pdf_key, pdf_path)

            return {
                "status": "ok",
                "pdf": pdf_s3
            }
        except subprocess.CalledProcessError as e:
            print("Error en la conversión:", e.stderr.decode())
            raise
        finally:
            cleanup(tmpdir)

def parse_event(event):
    # Soporta: evento S3 (Records) o dict con bucket/key
    if 'Records' in event and len(event['Records'])>0:
        rec = event['Records'][0]
        bucket = rec['s3']['bucket']['name']
        key = unquote_plus(rec['s3']['object']['key'])
    else:
        bucket = event.get('bucket')
        key = event.get('key')
    return bucket, key

def download_s3(bucket, key, local_path):
    s3.download_file(bucket, key, local_path)
    return local_path

def extract_images_from_docx(docx_path, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    images = []
    with zipfile.ZipFile(docx_path, 'r') as z:
        for name in z.namelist():
            if name.startswith('word/media/'):
                filename = os.path.basename(name)
                target = os.path.join(out_dir, filename)
                with z.open(name) as src, open(target, 'wb') as dst:
                    shutil.copyfileobj(src, dst)
                images.append(target)
    return images

def convert_docx_to_pdf(input_path, out_dir, timeout=120):
    os.makedirs(out_dir, exist_ok=True)
    # find libreoffice binary
    so = shutil.which('libreoffice') or shutil.which('soffice')
    if not so:
        raise RuntimeError("LibreOffice not found in PATH")
    cmd = [so, '--headless', '--convert-to', 'pdf', input_path, '--outdir', out_dir]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    # Determine output pdf path
    base = os.path.splitext(os.path.basename(input_path))[0]
    pdf_path = os.path.join(out_dir, base + '.pdf')
    if not os.path.exists(pdf_path):
        # Try alternative (sometimes libreoffice writes a different filename)
        # find any .pdf in out_dir newer than input
        candidates = [os.path.join(out_dir,f) for f in os.listdir(out_dir) if f.lower().endswith('.pdf')]
        if candidates:
            candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            pdf_path = candidates[0]
    return pdf_path

def upload_file(bucket, key, local_path):
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"

def cleanup(path):
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"Error cleaning up {path}: {e}")
