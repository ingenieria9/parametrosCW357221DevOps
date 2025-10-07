from constructs import Construct
from aws_cdk import aws_lambda as _lambda, aws_iam as iam

#Constructor que crea las 3 funciones lambdas de la fase de generación de entregables
#y les asigna los permisos necesarios para su funcionamiento

class LambdasFileGenConstruct(Construct):
    def __init__(self, scope: Construct, id: str, bucket, folder_name: str, project_name: str,  db_access_lambda_arn: str,):
        super().__init__(scope, id)

        # "formato" lambda
        self.formato = _lambda.Function(
            self,
            f"{project_name}-Formato-{folder_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/{folder_name}/formato"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "FOLDER_NAME": folder_name,
                "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
            },
            function_name=f"{project_name}-Formato-{folder_name}",
        )

        # "Informe" lambda
        self.informe = _lambda.Function(
            self,
            f"{project_name}-Informe-{folder_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/{folder_name}/informe"),
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "FOLDER_NAME": folder_name,
                "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
            },
            function_name=f"{project_name}-Informe-{folder_name}",
        )

        # Permisos S3
        bucket.grant_read_write(self.formato)
        bucket.grant_read_write(self.informe)

        # Lambda general entregables
        self.entregable = _lambda.Function(
            self,
            f"{project_name}-Entregable-{folder_name}",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=_lambda.Code.from_asset(f"../src/{folder_name}/entregable"),
            environment={
                "formato_ARN": self.formato.function_arn,
                "informe_ARN": self.informe.function_arn,
                "BUCKET_NAME": bucket.bucket_name,
                "FOLDER_NAME": folder_name,
                "DB_ACCESS_LAMBDA_ARN": db_access_lambda_arn,
            },
            function_name=f"{project_name}-Entregable-{folder_name}",
        )

        # Permisos de invocación
        self.formato.grant_invoke(self.entregable)
        self.informe.grant_invoke(self.entregable)


        # Importar la lambda remota
        db_access_lambda = _lambda.Function.from_function_arn(
            self,
            "ImportedDBAccessLambda",
            db_access_lambda_arn,
        )

        # Permite que formato, entregable e informe la invoque
        # Permitir invocación de la Lambda remota (por ARN) a cada Lambda local
        invoke_policy = iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[db_access_lambda_arn],
        )

        self.formato.add_to_role_policy(invoke_policy)
        self.entregable.add_to_role_policy(invoke_policy)
        self.informe.add_to_role_policy(invoke_policy)
