import os
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_ecr as ecr,
    RemovalPolicy,
    Duration,
    CfnOutput
)
from constructs import Construct
from datetime import datetime
image_tag = os.getenv("IMAGE_TAG", "latest")

class LambdaEcrS3TriggerStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, bucket_name: str, bucket_arn: str, project_name: str , **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Repositorio ECR existente
        repository = ecr.Repository.from_repository_name(
            self,
            "LambdaImageRepo",
            "libreoffice-converter",  
        )

        # Importar el bucket por nombre/ARN (no se crea relación circular)
        bucket = s3.Bucket.from_bucket_attributes(
            self,
            "ImportedBucket",
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
        )

        # Lambda basada en imagen ECR existente
        lambda_fn = _lambda.DockerImageFunction(
            self,
            "EcrLambda",
            function_name=f"{project_name}-file-converter",
            code=_lambda.DockerImageCode.from_ecr(
                repository,
                tag_or_digest=image_tag,
            ),
            memory_size=512,
            timeout=Duration.seconds(120),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "HOME": "/tmp",
                "DEPLOY_TIME": datetime.utcnow().isoformat() #force deployment  
            },
        )

        # Permisos de acceso al bucket
        bucket.grant_read_write(lambda_fn)

        # Trigger S3 → Lambda (solo para archivos DOCX en files-to-convert/)
        notification = s3n.LambdaDestination(lambda_fn)
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED_PUT,
            notification,
            s3.NotificationKeyFilter(
                prefix="files/files-to-convert/"
                #suffix=".docx",
            ),
        )

        # Outputs
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "LambdaName", value=lambda_fn.function_name)
