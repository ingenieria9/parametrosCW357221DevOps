import base64
import json
import os

password_A = os.environ["password_A"]
password_B = os.environ["password_B"]

#
def lambda_handler(event, context):
    try:
        # Obtener las credenciales del encabezado de autorización
        authorization_header = event['headers'].get('authorization', '')
        if not authorization_header.startswith('Basic '):
            return {"isAuthorized": False}
        
        credentials_encoded = authorization_header.split('Basic ')[1]
        decoded_credentials = base64.b64decode(credentials_encoded).decode('ascii')
        print(decoded_credentials)
        username, password = decoded_credentials.split(':', 1)

        # Lista de credenciales autorizadas
        authorized_user_credentials = [
            {"username": "admin", "password": password_A},
            {"username": "basic", "password": password_B},
        ]

        # Verificar si el usuario está autorizado
        is_authorized = any(
            user['username'] == username and user['password'] == password
            for user in authorized_user_credentials
        )

        # Devolver la respuesta en el formato esperado
        return {
            "isAuthorized": is_authorized
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"isAuthorized": False}