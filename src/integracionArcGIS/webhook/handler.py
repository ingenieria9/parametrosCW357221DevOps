def lambda_handler(event, context):
    print("Evento recibido:", event)
    return {
        'statusCode': 200,
        'body': '{"status": "ok"}',
        'headers': {
            'Content-Type': 'application/json'
        }
    }
