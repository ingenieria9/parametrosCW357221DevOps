from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_apigateway as apigw,
)
from constructs import Construct


class LambdaApiStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Lambda Function
        hello_fn = _lambda.Function(
            self,
            "HelloHandler",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset("../src"),
            function_name="cdk-demo-hello",
        )

        # API Gateway
        api = apigw.LambdaRestApi(
            self,
            "Endpoint",
            handler=hello_fn,
            proxy=False,
            rest_api_name="cdk-demo-api",
        )

        # Create /hello endpoint
        items = api.root.add_resource("hello")
        items.add_method("GET")  # GET /hello

        # Output the API URL
        self.api_url = api.url
