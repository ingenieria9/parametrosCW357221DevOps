from constructs import Construct
from aws_cdk import aws_lambda as _lambda

#Constructor que crea las 3 funciones lambdas de la fase de generación de entregables
#y les asigna los permisos necesarios para su funcionamiento

class LambdaPipelineConstruct(Construct):
    def __init__(self, scope: Construct, id: str, bucket, folder_name: str, project_name: str):
        super().__init__(scope, id)

        # Lambda B
        self.lambda_b = _lambda.Function(
            self,
            f"{project_name}-LambdaB-{folder_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/{folder_name}/lambda_b"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "FOLDER_NAME": folder_name,
            },
            function_name=f"{project_name}-LambdaB-{folder_name}",
        )

        # Lambda C
        self.lambda_c = _lambda.Function(
            self,
            f"{project_name}-LambdaC-{folder_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/{folder_name}/lambda_c"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "FOLDER_NAME": folder_name,
            },
            function_name=f"{project_name}-LambdaC-{folder_name}",
        )

        # Permisos S3
        bucket.grant_read_write(self.lambda_b)
        bucket.grant_read_write(self.lambda_c)

        # Lambda A
        self.lambda_a = _lambda.Function(
            self,
            f"{project_name}-LambdaA-{folder_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/{folder_name}/lambda_a"),
            environment={
                "LAMBDA_B_ARN": self.lambda_b.function_arn,
                "LAMBDA_C_ARN": self.lambda_c.function_arn,
                "BUCKET_NAME": bucket.bucket_name,
                "FOLDER_NAME": folder_name,
            },
            function_name=f"{project_name}-LambdaA-{folder_name}",
        )

        # Permisos de invocación
        self.lambda_b.grant_invoke(self.lambda_a)
        self.lambda_c.grant_invoke(self.lambda_a)
