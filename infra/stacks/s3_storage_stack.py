from aws_cdk import Stack, RemovalPolicy, aws_s3 as s3
from constructs import Construct


# Stack de almacenamiento S3 (Bucket del proyecto)

class StorageStack(Stack):
    def __init__(self, scope: Construct, id: str, project_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.bucket = s3.Bucket(
            self,
            f"{project_name}-SharedBucket",
            bucket_name=f"{project_name.lower()}-bucket",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )