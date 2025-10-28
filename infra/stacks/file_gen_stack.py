from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_lambda as _lambda
from .lambdas_file_gen_construct import LambdasFileGenConstruct

class FileGenStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket, project_name: str, db_access_lambda_arn: str,  openpyxl_layer: _lambda.ILayerVersion,
        docxtpl_layer: _lambda.ILayerVersion, requests_layer: _lambda.ILayerVersion, **kwargs):
        super().__init__(scope, id, **kwargs)

        self.pipeline1 = LambdasFileGenConstruct(
            self, f"{project_name}-fileGenFase1",
            bucket=bucket,
            folder_name="Fase1",
            project_name=project_name,
            db_access_lambda_arn = db_access_lambda_arn, 
            openpyxl_layer = openpyxl_layer ,
            docxtpl_layer = docxtpl_layer,
            requests_layer = requests_layer
        )
        
        self.entregable_fase1_lambda = self.pipeline1.entregable 

        '''
        self.pipeline2 = LambdasFileGenConstruct(
            self, f"{project_name}-fileGenFase2",
            bucket=bucket,
            folder_name="Fase2",
            project_name=project_name,
            db_access_lambda_arn=db_access_lambda_arn
        )
        
        self.pipeline3 = LambdasFileGenConstruct(
            self, f"{project_name}-fileGenFase3",
            bucket=bucket,
            folder_name="Fase3",
            project_name=project_name,
            db_access_lambda_arn=db_access_lambda_arn
        )
        '''