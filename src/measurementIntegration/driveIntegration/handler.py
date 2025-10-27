import os
import json
import tempfile
import logging
import boto3
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === ENV VARS ===
S3_BUCKET = os.environ.get('S3_BUCKET')
DRIVE_FOLDER_ID = 'xxx'
PARAM_KEY_SA = os.environ.get('PARAM_KEY_SA', '/dev/drive/service_account_key')
PARAM_KEY_TOKEN = os.environ.get('PARAM_KEY_TOKEN', '/dev/drive/start_page_token')
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# === AWS CLIENTS ===
ssm = boto3.client('ssm')
s3 = boto3.client('s3')

# === Helper functions ===
def get_param(name, decrypt=False):
    resp = ssm.get_parameter(Name=name, WithDecryption=decrypt)
    return resp['Parameter']['Value']

def put_param(name, value):
    ssm.put_parameter(
        Name=name,
        Value=value,
        Type='String',
        Overwrite=True
    )

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

# === Lambda handler ===
def lambda_handler(event, context):
    print("Lambda started - checking Google Drive changes")

    drive_service = get_drive_service()

    # Get token or create new
    try:
        token = get_param(PARAM_KEY_TOKEN)
        if not token:
            token = drive_service.changes().getStartPageToken().execute().get('startPageToken')
            put_param(PARAM_KEY_TOKEN, token)
            print(f"Initialized startPageToken: {token}")
    except Exception as e:
        logger.warning("Could not get startPageToken from SSM: %s", e)
        token = drive_service.changes().getStartPageToken().execute().get('startPageToken')
        put_param(PARAM_KEY_TOKEN, token)

    print(f"Using startPageToken={token}")

    # List changes
    request = drive_service.changes().list(
        pageToken=token,
        spaces='drive',
        fields='nextPageToken,newStartPageToken,changes(fileId,file(name,mimeType,parents))',
        includeRemoved=False,
        supportsAllDrives=True
    )

    all_changes = []
    while request is not None:
        response = request.execute()
        all_changes.extend(response.get('changes', []))
        request = drive_service.changes().list_next(request, response)

    print(f"Found {len(all_changes)} changes since last token")

    processed = []
    for change in all_changes:
        file = change.get('file')
        if not file:
            continue
        name = file.get('name')
        mime = file.get('mimeType')
        if mime == 'application/vnd.google-apps.folder':
            continue

        if (mime == 'text/csv' or name.lower().endswith('.csv')) and is_in_folder(file, DRIVE_FOLDER_ID):
            file_id = file['id']
            print(f"Downloading new CSV: {name}")
            tmp = download_file(drive_service, file_id, name)
            s3_key = f"drive_uploads/{name}"
            s3.upload_file(tmp, S3_BUCKET, s3_key)
            processed.append({'file': name, 's3_key': s3_key})
            print(f"Uploaded {name} to s3://{S3_BUCKET}/{s3_key}")

    # Update token
    new_token = drive_service.changes().getStartPageToken().execute().get('startPageToken')
    put_param(PARAM_KEY_TOKEN, new_token)
    print(f"Updated startPageToken to {new_token}")

    return {
        'status': 'ok',
        'processed_files': processed,
        'new_startPageToken': new_token
    }
