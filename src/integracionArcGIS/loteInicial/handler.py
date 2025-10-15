def lambda_handler(event, context):
    """
    This function demonstrates a simple "Hello World" message in AWS Lambda.
    """
    return {
        'statusCode': 200,
        'body': 'Hello World'
    }