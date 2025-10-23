import os
import json
import requests

BUCKET_NAME = os.environ.get("BUCKET_NAME", "no-bucket")

def lambda_handler(event, context):
    """
    Lambda simple para probar el uso de la librería 'requests' desde una Layer.
    """
    print(f"📦 Bucket configurado: {BUCKET_NAME}")

    try:
        # Llamada HTTP simple para verificar que 'requests' funciona
        response = requests.get("https://api.github.com")
        data = response.json()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "bucket_name": BUCKET_NAME,
                "github_api_status": data.get("current_user_url", "ok"),
            })
        }

    except Exception as e:
        print(f"❌ Error usando requests: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
