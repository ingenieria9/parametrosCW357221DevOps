from aws_cdk import Stack
from constructs import Construct
from .lambda_pipeline_construct import LambdaPipelineConstruct

class ApplicationStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket, project_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.pipeline1 = LambdaPipelineConstruct(
            self, f"{project_name}-PipelineFase1",
            bucket=bucket,
            folder_name="Fase1",
            project_name=project_name,
        )

        self.pipeline2 = LambdaPipelineConstruct(
            self, f"{project_name}-PipelineFase2",
            bucket=bucket,
            folder_name="Fase2",
            project_name=project_name,
        )

        self.pipeline3 = LambdaPipelineConstruct(
            self, f"{project_name}-PipelineFase3",
            bucket=bucket,
            folder_name="Fase3",
            project_name=project_name,
        )
