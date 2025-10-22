import os

BUCKET_NAME = os.environ["BUCKET_NAME"]

def lambda_handler(event, context):
    """
    This function demonstrates a simple "Hello World" message in AWS Lambda.
    """
    print(BUCKET_NAME)
    return {
        'statusCode': 200,
        'body': BUCKET_NAME
    }