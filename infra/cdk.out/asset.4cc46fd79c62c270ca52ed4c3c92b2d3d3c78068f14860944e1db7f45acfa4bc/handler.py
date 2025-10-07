import boto3
import json

def lambda_handler(event, context):
    
    client = boto3.client("lambda", region_name="us-east-1")  # 👈 región donde vive la DB Access
    db_access_arn = os.environ["DB_ACCESS_LAMBDA_ARN"]

    response = client.invoke(
        FunctionName=db_access_arn,
        InvocationType="RequestResponse",
        Payload=json.dumps({"action": "query", "data": "..."})
    )

    result = json.load(response["Payload"])
    return result
