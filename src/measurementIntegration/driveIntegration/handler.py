import os
import json
import tempfile
import logging
import boto3
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime, timezone


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === ENV VARS ===
S3_BUCKET = os.environ.get('BUCKET_NAME')
DRIVE_FOLDER_ID = '1nnqlexiEPGtTeEaqoTdNZLxydNlZAViV'
PARAM_KEY_SA = os.environ.get('PARAM_KEY_SA', '/dev/drive/service_account_key')
PARAM_KEY_TOKEN = os.environ.get('PARAM_KEY_TOKEN', '/dev/drive/start_page_token')
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# === AWS CLIENTS ===
ssm = boto3.client('ssm')
s3 = boto3.client('s3')

INDEX_KEY = "Campo_Data_Uploads/index.json"


# === Helper functions ===
def get_param(name, decrypt=False):
    resp = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return resp['Parameter']['Value']

def put_param(name, value):
    ssm.put_parameter(Name=name,Value=value,Type='String',Overwrite=True)

def get_drive_service():
    # load SA key JSON
    sa_json = get_param(PARAM_KEY_SA, decrypt=True)
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

def download_file(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    temp_path = os.path.join(tempfile.gettempdir(), file_name)
    with open(temp_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    return temp_path

def is_in_folder(file_meta, folder_id):
    return folder_id in file_meta.get('parents', [])

def load_index():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="Campo_Data_Uploads/index.json")
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {"CIRCUITOS": {}, "CUENCAS": {}}


def save_index(index):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="Campo_Data_Uploads/index.json",
        Body=json.dumps(index).encode("utf-8"),
        ContentType="application/json"
    )

def sanitize(name):
    """Replace spaces and problematic chars for S3"""
    return name.replace(" ", "_")


def path_to_s3(path_list):
    # Reemplaza espacios por "_"
    cleaned = [p.replace(" ", "_") for p in path_list]
    return "/".join(cleaned)

def build_drive_path(drive_service, file_meta):
    """
    Reconstruye la ruta completa del archivo desde DRIVE_FOLDER_ID hacia abajo.
    Devuelve por ejemplo:
        ["CIRCUITOS", "AMERICA", "file.csv"]
    Si el archivo NO está dentro de DRIVE_FOLDER_ID, retorna None.
    """
    path = [file_meta["name"]]

    parents = file_meta.get("parents", [])
    if not parents:
        return None

    current = parents[0]

    while True:

        folder = drive_service.files().get(
            fileId=current,
            fields="id, name, parents"
        ).execute()

        folder_name = folder["name"]

        if folder["id"] == DRIVE_FOLDER_ID:
            return path[::-1]  # invertir lista (de root → archivo)

        path.append(folder_name)

        parent_list = folder.get("parents", [])
        if not parent_list:
            return None  # llegó al root del Drive y nunca encontró DRIVE_FOLDER_ID

        current = parent_list[0]

def is_descendant_of(drive_service, file_meta, root_folder_id):
    parents = file_meta.get("parents", [])
    if not parents:
        return False

    current = parents[0]

    while True:
        if current == root_folder_id:
            return True

        # Get parent folder metadata
        folder = drive_service.files().get(
            fileId=current,
            fields="id, parents"
        ).execute()

        parent_list = folder.get("parents", [])
        if not parent_list:
            return False  # Reached root of Drive without matching root_folder_id

        current = parent_list[0]


# === Lambda handler ===
def lambda_handler(event, context):
    print("Lambda started - checking Google Drive changes")

    drive = get_drive_service()
    index = load_index()

    for key in ("CIRCUITOS", "CUENCAS"):
        if key not in index or not isinstance(index[key], dict):
            index[key] = {}

    # --- TOKEN ---
    try:
        token = get_param(PARAM_KEY_TOKEN)
        if not token:
            token = drive.changes().getStartPageToken().execute()["startPageToken"]
            put_param(PARAM_KEY_TOKEN, token)
            print("Initialized startPageToken:", token)
    except Exception:
        token = drive.changes().getStartPageToken().execute()["startPageToken"]
        put_param(PARAM_KEY_TOKEN, token)

    print("Using startPageToken:", token)

    # --- LIST CHANGES ---
    req = drive.changes().list(
        pageToken=token,
        fields='nextPageToken,newStartPageToken,changes(fileId,file(id,name,mimeType,parents))',
        includeRemoved=False,
        supportsAllDrives=True
    )

    all_changes = []
    while req is not None:
        resp = req.execute()
        all_changes.extend(resp.get("changes", []))
        req = drive.changes().list_next(req, resp)

    print(f"Found {len(all_changes)} changes")

    processed = []

    # === PROCESAR CAMBIOS ===
    for change in all_changes:
        file = change.get("file")
        if not file:
            continue

        mime = file.get("mimeType")
        name = file.get("name")

        # Filtrar solo archivos CSV
        if mime == "application/vnd.google-apps.folder":
            continue
        if not (mime == "text/csv" or name.lower().endswith(".csv")):
            continue

        # Reconstruir ruta completa en Drive
        path_list = build_drive_path(drive, file)
        if not path_list:
            continue  # No pertenece a DRIVE_FOLDER_ID

        # path_list es algo como: ["CIRCUITOS", "AMERICA", "file.csv"]
        root = path_list[0]  # CIRCUITOS o CUENCAS

        if root not in ("CIRCUITOS", "CUENCAS"):
            continue

        path_s3 = path_to_s3(["Campo_Data_Uploads"] + path_list)

        # Verificar si ya está procesado
        flat_path = "/".join(path_list)  # "CIRCUITOS/AMERICA/file.csv"
        if flat_path in index[root]:
            print("Already processed:", flat_path)
            continue

        # Descargar + subir a S3
        print("Downloading:", name)
        tmp = download_file(drive, file["id"], name)
        s3.upload_file(tmp, S3_BUCKET, path_s3)

        # Agregar al índice
        index[root][flat_path] = True


        processed.append(path_s3)
        print("Uploaded to:", path_s3)

    # Guardar index.json actualizado
    save_index(index)

    # Actualizar startPageToken
    new_token = drive.changes().getStartPageToken().execute()["startPageToken"]
    put_param(PARAM_KEY_TOKEN, new_token)

    print("Updated startPageToken:", new_token)

    return {
        "status": "ok",
        "processed_files": processed,
        "new_startPageToken": new_token
    }