#This stacks creates an API gateway V2, with authentication with lambda authorizer
# it also creates one basic lambda function that will be invoked by the API gateway
# This API will invoke other lambdas in other stacks

from aws_cdk import (
    CfnOutput,
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
    aws_apigatewayv2_authorizers as authorizers,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    aws_s3 as s3, 
)
from constructs import Construct

import os

password_API_A = os.environ["password_API_A"]
password_API_B = os.environ["password_API_B"]

class ApiGenStack(Stack):
    # bucket, project_name: str, db_access_lambda_arn, lambdas_gen_files (list)
    def __init__(self, scope: Construct, id: str , project_name: str, lambda_changes: str, lambda_sendFile: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # API V2
        http_api = apigwv2.HttpApi(self, f"{project_name}GenerationAPI",)
        self.api_url = http_api.api_endpoint

        # ======================================================
        # Lambda: Authorizer Function
        # ======================================================
        authorizer_lambda = _lambda.Function( 
            self,
            "AuthorizerLambda",
            function_name=f"{project_name}-authorizer",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/apiGen/Authorizer/"),
            environment={
                "password_A" : password_API_A,
                "password_B" : password_API_B,
                },
            timeout=Duration.seconds(5),
        )
        
        lambda_authorizer = authorizers.HttpLambdaAuthorizer(
            "ApiLambdaAuthorizer",
            authorizer_lambda=authorizer_lambda,
            authorizer_name=f"{project_name}-lambda-authorizer",
            identity_source=["$request.header.Authorization"],
            response_types=[authorizers.HttpLambdaResponseType.SIMPLE],
        )


        # ======================================================
        # Lambda: basic call
        # ======================================================
        simple_get = _lambda.Function(
        self, 
        "BasicGetLambda" ,
        function_name= f"{project_name}-basicGet",
        runtime=_lambda.Runtime.PYTHON_3_13,
        handler="handler.lambda_handler",
        #code basic hello world
        code=_lambda.Code.from_inline
        (
            """def lambda_handler(event, context):
                return {
                    'statusCode': 200,
                    'body': 'Hello from basic get!'
                }
            """
        ),
        timeout=Duration.seconds(3))


        # IMPORT LAMBDA FUNCTIONS TO BE INVOKED BY THE API
        lambda_changes_fn = _lambda.Function.from_function_arn(
            self, "ImportedLambdaChanges", lambda_changes
        )

        lambda_send_fn = _lambda.Function.from_function_arn(
            self, "ImportedLambdaSendFile", lambda_sendFile
        )

        # PERMISSIONS
        simple_get.add_permission(
            "AllowHttpApiInvokeBasicGet",
            principal=_lambda.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"{http_api.api_id}/*"
        )

        lambda_changes_fn.add_permission(
            "AllowHttpApiInvokeChanges",
            principal=_lambda.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"{http_api.api_id}/*"  # o usa api.execution_arn
        )

        lambda_send_fn.add_permission(
            "AllowHttpApiInvokeSendFile",
            principal=_lambda.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"{http_api.api_id}/*"
        )

        #INTEGRATIONS

        simple_get_integration = integrations.HttpLambdaIntegration(
            "SimpleGetIntegration",
              simple_get 
        )
        
        lambda_changes_integration = integrations.HttpLambdaIntegration(
            "LambdaChangesIntegration",
            lambda_changes_fn
        )

        lambda_send_integration = integrations.HttpLambdaIntegration(
            "LambdaSendIntegration",
            lambda_send_fn
        )

        # ROUTES
        http_api.add_routes(
            path="/",
            methods=[apigwv2.HttpMethod.GET],
            integration=simple_get_integration,
            authorizer=lambda_authorizer
        )

        http_api.add_routes(
            path="/changes",
            methods=[apigwv2.HttpMethod.POST],
            integration=lambda_changes_integration,
            authorizer=lambda_authorizer
        )

        http_api.add_routes(
            path="/sendFile",
            methods=[apigwv2.HttpMethod.POST],
            integration=lambda_send_integration,
            authorizer=lambda_authorizer
        )



