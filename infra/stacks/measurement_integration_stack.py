
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    aws_s3 as s3, 
    aws_ssm as ssm,
    aws_s3_notifications as s3n
)
from constructs import Construct
import os

class MeasurementIntStack(Stack):
    # bucket, project_name: str, db_access_lambda_arn, lambdas_gen_files (list)
    def __init__(self, scope: Construct, id: str, bucket_name: str, bucket_arn:str , project_name: str, drive_layer: _lambda.ILayerVersion, db_access_lambda_arn: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Importar el bucket por nombre/ARN (no se crea relación circular)
        bucket = s3.Bucket.from_bucket_attributes(
            self,
            "ImportedBucket",
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
        )

        # ======================================================
        # SSM --> Parámetro: Service Account JSON
        # ======================================================
        service_account_param = ssm.StringParameter(
            self,
            "DriveServiceAccountParam",
            parameter_name=f"/{project_name}/drive/service_account_key",
            string_value="x",
            description="Google Service Account JSON for Drive integration"
        )

        # ======================================================
        # Parámetro: startPageToken (String)
        # ======================================================
        start_token_param = ssm.StringParameter(
            self,
            "DriveStartTokenParam",
            parameter_name=f"/{project_name}/drive/start_page_token",
            string_value="x",
            description="Drive startPageToken used for incremental sync"
        )


        # ======================================================
        # Lambda: driveIntegration
        # ======================================================
        drive_integration_lambda = _lambda.Function(
            self, 
            "driveIntegrationLambda" ,
            function_name= f"{project_name}-driveIntegration",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/measurementIntegration/driveIntegration"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "PARAM_KEY_SA": service_account_param.parameter_name,
                "PARAM_KEY_TOKEN": start_token_param.parameter_name
            },
            layers=[drive_layer],
            timeout=Duration.seconds(60)
        )            

        # Acceso al bucket
        bucket.grant_read_write(drive_integration_lambda)
        # ======================================================
        # Permisos: Lambda puede leer/escribir los parámetros
        # ======================================================
        service_account_param.grant_read(drive_integration_lambda)
        start_token_param.grant_read(drive_integration_lambda)
        start_token_param.grant_write(drive_integration_lambda)

        events.Rule(
            self, "cronMeasurementIntegration",  rule_name = f"{project_name}-cronMeasurementIntegration",
            schedule=events.Schedule.cron(
                minute="30",
                hour="17,21",  # 12:30 pm UTC-5 y 4:30 pm UTC-5
                week_day="MON-SAT"
            ),
            targets=[targets.LambdaFunction(drive_integration_lambda)],
            enabled = False
        )           

        # Por crear: Lambda que se triggerea por put object a s3 (carpeta drive_uploads/).
        # Esta lambda tiene permisos para llamar a db_access_lambda_arn y asi almacenar 
        # y procesar la información en la DB. 

        # ======================================================
        # Lambda: uploadData
        # ======================================================
        upload_data_lambda = _lambda.Function(
            self, 
            "uploadDataLambda" ,
            function_name= f"{project_name}-uploadData",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/measurementIntegration/uploadData"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "DB_ACCESS_LAMBDA_ARN" : db_access_lambda_arn
            },
            reserved_concurrent_executions=5,
            timeout=Duration.seconds(180)
        )

        # Permiso para invocar db_access_lambda
        upload_data_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[db_access_lambda_arn]
            )
        ) 

        # Trigger S3 → Lambda (solo para archivos CSV en Campo_Data_Uploads/)
        notification = s3n.LambdaDestination(upload_data_lambda)
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED_PUT,
            notification,
            s3.NotificationKeyFilter(
                prefix="Campo_Data_Uploads/",
                suffix=".csv",
            )
        )        