from aws_cdk import (
    Stack,
    aws_ec2 as ec2,
    aws_lambda as _lambda,
    RemovalPolicy,
    CfnOutput,
    aws_iam,
)
from constructs import Construct


class DbAccessStack(Stack):
    def __init__(self, scope: Construct, id: str, project_name: str, vpc_id: str, psycopg2_layer_arn: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # VPC existente
        vpc = ec2.Vpc.from_lookup(
            self,
            f"{project_name}-ExistingVpc",
            vpc_id=vpc_id,
        )

        # Referenciar la Layer existente (por ARN)
        psycopg2_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            f"{project_name}-Psycopg2Layer",
            layer_version_arn=psycopg2_layer_arn,
        )

        # Crear la Lambda dentro de esa VPC, con la Layer
        db_access_fn = _lambda.Function(
            self,
            f"{project_name}-DbAccessLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="db_access.lambda_handler",
            code=_lambda.Code.from_asset("../src/db_access"),
            allow_public_subnet=True,
            vpc=vpc,
            layers=[psycopg2_layer],
            function_name=f"{project_name}-db-access",
            environment={
                "DB_HOST": "your-db-host.rds.amazonaws.com",
                "DB_USER": "dbuser",
                "DB_PASSWORD": "arn:aws:secretsmanager:us-east-1:123456789012:secret:dbcreds",
                "ENABLE_LOGS": "false",  # opcional
            },
        )

        # Evitar logs innecesarios en CloudWatch
        db_access_fn.role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=["*"],
                effect=aws_iam.Effect.DENY,
            )
        )

        # db_access_fn.add_environment("ENABLE_LOGS", "false")

        # Exportar el ARN para usarlo en otros stacks
        CfnOutput(
            self,
            "DbAccessLambdaArn",
            value=db_access_fn.function_arn,
            export_name=f"{project_name}-DbAccessLambdaArn",
        )

        self.db_access_lambda_arn = db_access_fn.function_arn

        db_access_fn.add_permission(
            "AllowInvocationFromOtherRegionLambdas",
            principal=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_account=self.account
        )
