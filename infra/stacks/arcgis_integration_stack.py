# stack que genera:
'''

Variables input del stack
bucket, project_name: str, db_access_lambda_arn, entregables-fase-x (lista)

1. API Gateway + lambda (changes)
    - ruta: Post a /update
    - Acceso a bucket
    - permiso de invocar a lambda infoUpdate
2. Lambda infoUpdte
    - acceso al bucket
    - permiso de invocar a lambda db_access_lambda_arn 
    - permiso de invocar a lambdas entregables-fase-x
3. Lambda lote inicial
    - acceso al bucket
    - permiso de invocar a lambda db_access_lambda_arn     
4. Lambda update semanal trigger by eventbridge scheduler
    - acceso al bucket
'''

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
    CfnOutput
)
from constructs import Construct
import os

ARCGIS_CLIENT_ID = os.environ["ARCGIS_CLIENT_ID"]
ARCGIS_CLIENT_SECRET = os.environ["ARCGIS_CLIENT_SECRET"]

class ArcGISIntStack(Stack):
    # bucket, project_name: str, db_access_lambda_arn, lambdas_gen_files (list)
    def __init__(self, scope: Construct, id: str, bucket_name: str, bucket_arn:str , project_name: str, db_access_lambda_arn: str, entregables_fase_x: list, request_layer: _lambda.ILayerVersion, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Referenciar la Layer existente (por ARN)
        '''request_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            f"{project_name}-request_layer",
            layer_version_arn="arn:aws:lambda:us-west-2:339713063336:layer:request_libraryforHTTP:1",
        )'''

        # Importar el bucket por nombre/ARN (no se crea relación circular)
        bucket = s3.Bucket.from_bucket_attributes(
            self,
            "ImportedBucket",
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
        )


        # ======================================================
        # Lambda: infoUpdate
        # ======================================================
        info_update_lambda = _lambda.Function(
            self, 
            "InfoUpdateLambda" ,
            function_name= f"{project_name}-infoUpdate",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/integracionArcGIS/infoUpdate"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
                "ENTREGABLES_FASE_X": ",".join(entregables_fase_x),
                "ARCGIS_CLIENT_ID" : ARCGIS_CLIENT_ID,
                "ARCGIS_CLIENT_SECRET" : ARCGIS_CLIENT_SECRET
            },
            layers=[request_layer],
            timeout=Duration.seconds(120)
        )

        # Acceso al bucket
        bucket.grant_read_write(info_update_lambda)

        # Permiso de invocar lambdas externas
        info_update_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[db_access_lambda_arn] + entregables_fase_x
            )
        )

        # ======================================================
        # Lambda: Changes
        # ======================================================
        self.changes_lambda = _lambda.Function(
            self, 
            "ChangesLambda" ,
            function_name= f"{project_name}-ArcGISChanges",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/integracionArcGIS/changes"),
            environment={"BUCKET_NAME": bucket.bucket_name,
                        "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
                        "LAMBDA_INFO_UPDATE": info_update_lambda.function_arn,
                        "ARCGIS_CLIENT_ID" : ARCGIS_CLIENT_ID,
                        "ARCGIS_CLIENT_SECRET" : ARCGIS_CLIENT_SECRET
                        },
            layers=[request_layer],
            timeout=Duration.seconds(20)                        
        )
        CfnOutput(self, "ChangesLambdaArn", value=self.changes_lambda.function_arn)

        # acceso al bucket
        bucket.grant_read_write(self.changes_lambda)
        #changes_lambda.add_to_role_policy(
        #    iam.PolicyStatement(
        #        actions=["s3:*"],
        #        resources=[f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
        #    )
        #)
        # permiso para invocar info_update_lambda
        info_update_lambda.grant_invoke(self.changes_lambda)

        # EventBridge rule (1 vez al finalizar el dia (4 pm UTC-5))
        events.Rule(
            self, "changesArcgisInfo",  rule_name = f"{project_name}-changesArcgisInfo",
            schedule=events.Schedule.cron(
                minute="0",
                hour="21",  # 4pm UTC-5 is 21 UTC
                week_day="MON-SAT"
            ),
            targets=[targets.LambdaFunction(self.changes_lambda)]
        )
        
        # ======================================================
        # Lambda: lote_inicial
        # ======================================================
        lote_inicial_lambda = _lambda.Function(
            self, 
            "loteInicial" ,
            function_name= f"{project_name}-loteInicial",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/integracionArcGIS/loteInicial"),
            environment={"BUCKET_NAME": bucket.bucket_name,
                        "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
                        "ARCGIS_CLIENT_ID" : ARCGIS_CLIENT_ID,
                        "ARCGIS_CLIENT_SECRET" : ARCGIS_CLIENT_SECRET
                        },
            layers=[request_layer],
            timeout=Duration.seconds(300)                       
        )

        # Acceso al bucket
        bucket.grant_read_write(lote_inicial_lambda)

        # Permiso para invocar db_access_lambda
        lote_inicial_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[db_access_lambda_arn]
            )
        )

        # ======================================================
        # Lambda: update_semanal (EventBridge Trigger)
        # ======================================================
        update_semanal_lambda = _lambda.Function(
            self, "updateCronLambda",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/integracionArcGIS/updateCron"),
            environment={"BUCKET_NAME": bucket.bucket_name},
            function_name=f"{project_name}-updateCron",
            timeout=Duration.seconds(20),
            layers=[request_layer]
        )

        # Acceso al bucket
        bucket.grant_read_write(update_semanal_lambda)

        # EventBridge rule (1 vez por semana)
        events.Rule(
            self, "updateArcgisInfo",  rule_name = f"{project_name}-updateArcgisInfo",
            schedule=events.Schedule.rate(Duration.days(7)),
            targets=[targets.LambdaFunction(update_semanal_lambda)]
        )

   

