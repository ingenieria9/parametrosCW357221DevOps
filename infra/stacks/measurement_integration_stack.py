
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
    aws_ssm as ssm
)
from constructs import Construct
import os

class MeasurementIntStack(Stack):
    # bucket, project_name: str, db_access_lambda_arn, lambdas_gen_files (list)
    def __init__(self, scope: Construct, id: str, bucket_name: str, bucket_arn:str , project_name: str, drive_layer: _lambda.ILayerVersion, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Importar el bucket por nombre/ARN (no se crea relación circular)
        bucket = s3.Bucket.from_bucket_attributes(
            self,
            "ImportedBucket",
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
        )

        # ======================================================
        # Parámetro: Service Account JSON
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
