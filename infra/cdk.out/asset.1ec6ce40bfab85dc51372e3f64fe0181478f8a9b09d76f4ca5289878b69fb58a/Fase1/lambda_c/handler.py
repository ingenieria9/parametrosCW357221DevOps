def lambda_handler(event, context):
    return {
        "statusCode": 200,
        "body": f"Hola Mundo, desde Lambda {context.function_name}!",
    }