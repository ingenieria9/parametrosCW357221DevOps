'''
Stack que tiene: 
- Lambda:
    - con permisos para invocar a lambda db_access_lambda_arn
    - con layer requests_layer
    - permisos de acceder al bucket para buscar archivos 
    Variables de entorno:
    - BUCKET_NAME
    - DB_ACCESS_LAMBDA_ARN
    - GMAIL_EMAIL_PASSWORD
- Eventbridge scheduler trigger todos los dias 4pm
'''


from aws_cdk import (
    CfnOutput,
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    aws_s3 as s3, 
)
from constructs import Construct
import os

GMAIL_EMAIL_PASSWORD = os.getenv("GMAIL_EMAIL_PASSWORD", "")
#GMAIL_ORIGIN_EMAIL = os.getenv("GMAIL_ORIGIN_EMAIL", "")
#GMAIL_RECIPIENT_EMAIL = os.getenv("GMAIL_RECIPIENT_EMAIL", "")


class FileSendStack(Stack):
    # bucket, project_name: str, db_access_lambda_arn, lambdas_gen_files (list)
    def __init__(self, scope: Construct, id: str, bucket_name: str, bucket_arn:str , project_name: str, db_access_lambda_arn: str, request_layer: _lambda.ILayerVersion, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Importar el bucket por nombre/ARN (no se crea relación circular)
        bucket = s3.Bucket.from_bucket_attributes(
            self,
            "ImportedBucket",
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
        )

        self.send_file_lambda = _lambda.Function(
            self, "sendFileLambda",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/sendFiles/sendFile"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
                "GMAIL_EMAIL_PASSWORD": GMAIL_EMAIL_PASSWORD,
                },
            function_name=f"{project_name}-sendFile",
            timeout=Duration.seconds(180),
            memory_size=256,
            layers=[request_layer]
        )

        # Acceso al bucket
        bucket.grant_read_write(self.send_file_lambda)

        # Permiso para invocar db_access_lambda
        self.send_file_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[db_access_lambda_arn]
            )
        )        

        # EventBridge rule (1 vez por semana)
        events.Rule(
            self, "sendDailyEmailRule",  rule_name = f"{project_name}-sendDailyEmailRule",
            #Every day at 4pm UTC-5 from Monday to Friday
            schedule=events.Schedule.cron(
                minute="20",
                hour="21",  # 4pm UTC-5 is 21 UTC
                week_day="MON-SAT"
            ),
            targets=[targets.LambdaFunction(self.send_file_lambda)]
        )
