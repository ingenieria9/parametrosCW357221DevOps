from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
)
from constructs import Construct
import os

class LayersStack(Stack):
    def __init__(self, scope: Construct, id: str, project_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        layers_path = os.path.join(os.path.dirname(__file__), "../layers")

        # Openpyxl Layer (desde ZIP)
        openpyxl_zip_path = os.path.join(layers_path, "openpyxl.zip")
        self.openpyxl_layer = _lambda.LayerVersion(
            self,
            f"{project_name}-OpenpyxlLayer",
            layer_version_name=f"{project_name}-openpyxl",
            code=_lambda.Code.from_asset(openpyxl_zip_path),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            description="Layer con OpenpyXL desde ZIP",
        )

        # Docxtpl Layer (desde ZIP)
        docxtpl_zip_path = os.path.join(layers_path, "docxtpl.zip")
        self.docxtpl_layer = _lambda.LayerVersion(
            self,
            f"{project_name}-DocxtplLayer",
            layer_version_name=f"{project_name}-docxtpl",
            code=_lambda.Code.from_asset(docxtpl_zip_path),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            description="Layer con DocxTpl desde ZIP",
        )

        # Requests Layer (desde ZIP)
        requests_zip_path = os.path.join(layers_path, "requests.zip")
        self.requests_layer = _lambda.LayerVersion(
            self,
            f"{project_name}-requestsLayer",
            layer_version_name=f"{project_name}-requests",
            code=_lambda.Code.from_asset(requests_zip_path),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            description="Layer con requests desde ZIP",
        )     

        # Google Layer (desde ZIP)
        google_zip_path = os.path.join(layers_path, "google.zip")
        self.google_layer = _lambda.LayerVersion(
            self,
            f"{project_name}-googleLayer",
            layer_version_name=f"{project_name}-google",
            code=_lambda.Code.from_asset(google_zip_path),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_13
            ],
            description="Layer con google desde ZIP",
        )               

        # Exponer los ARNs como outputs (para usar desde otros stacks)
        from aws_cdk import CfnOutput

        CfnOutput(self, "OpenpyxlLayerArn", value=self.openpyxl_layer.layer_version_arn)
        CfnOutput(self, "DocxtplLayerArn", value=self.docxtpl_layer.layer_version_arn)
        CfnOutput(self, "RequestsLayerArn", value=self.requests_layer.layer_version_arn)
        CfnOutput(self, "GoogleLayerArn", value=self.google_layer.layer_version_arn)
