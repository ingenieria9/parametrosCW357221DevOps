from aws_cdk import Stack
from constructs import Construct
from .lambda_pipeline_construct import LambdaPipelineConstruct

class ApplicationStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket, project_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.pipeline1 = LambdaPipelineConstruct(
            self, f"{project_name}-PipelineCaso1",
            bucket=bucket,
            folder_name="caso1",
            project_name=project_name,
        )

        self.pipeline2 = LambdaPipelineConstruct(
            self, f"{project_name}-PipelineCaso2",
            bucket=bucket,
            folder_name="caso2",
            project_name=project_name,
        )

        self.pipeline3 = LambdaPipelineConstruct(
            self, f"{project_name}-PipelineCaso3",
            bucket=bucket,
            folder_name="caso3",
            project_name=project_name,
        )
