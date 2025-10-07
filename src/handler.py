def lambda_handler(event, context):
    name = event.get("queryStringParameters", {}).get("name", "Mundo")
    return {
        "statusCode": 200,
        "body": f"Hola {name}, desde Lambda + API Gateway vía CDK!"
    }
