from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as integrations,
)
from constructs import Construct

class LambdaApiStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        hello_fn = _lambda.Function(
            self,
            "HelloHandler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("../src"),
            function_name="cdk-demo-hello",
        )

        integration = integrations.HttpLambdaIntegration(
            "LambdaIntegration",
            handler=hello_fn,
        )

        api = apigwv2.HttpApi(
            self,
            "HttpApi",
            default_integration=integration,
            api_name="cdk-demo-http-api",
        )

        self.api_url = api.api_endpoint
