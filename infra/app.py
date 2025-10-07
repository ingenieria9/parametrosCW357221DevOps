#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.lambda_api_stack import LambdaApiStack
from stacks.s3_storage_stack import StorageStack
from stacks.application_stack import ApplicationStack
from stacks.db_access_stack import DbAccessStack

app = cdk.App()

#Variables que se usaran para identificar los recursos
MAIN_NAME = "CW357221ParametrosDevOps"
ENV_NAME = "dev"
PROJECT_NAME = f"{MAIN_NAME}{ENV_NAME}"


MAIN_REGION = "us-west-2"
ACCOUNT = "339713063336"

#El proyecto usa una lambda de acceso a una DB. La DB existe en otra región,
#por lo que la lambda de acceso a la DB entra como variable de entorno para 
#las lambdas que la invocan

LAMBDA_DB_ACCESS = "arn:aws:lambda:us-east-1:123456789012:function:xxx"


# ------------- Stacks --------------------
# Cada stack representa un conjunto de recursos de AWS 


# Stack de prueba inicial: Api gateway v2 + lambda
LambdaApiStack(app, "LambdaApiStack", env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION))


#Stack que se despliega en N. Virginia (us-east-1) y contiene la lambda de acceso a la DB
# Parámetros de recursos existentes en us-east-1
VPC_ID = "vpc-062bfffdbc4877cb4"
PSYCOPG2_LAYER_ARN = "arn:aws:lambda:us-east-1:339713063336:layer:psycopg2:1"

# Stack de Lambda DB Access (en la región donde está la VPC y la Layer)
db_stack = DbAccessStack(
    app,
    f"{PROJECT_NAME}-DbAccessStack",
    project_name=PROJECT_NAME,
    vpc_id=VPC_ID,
    psycopg2_layer_arn=PSYCOPG2_LAYER_ARN,
    env=cdk.Environment(account=ACCOUNT, region="us-east-1"),
)


# Stack de almacenamiento S3 (Bucket del proyecto)
storage = StorageStack(app, f"{PROJECT_NAME}-StorageStack", project_name=PROJECT_NAME, env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION))

# Stack para paso 2 "Generación de entregables" para Fase I, II  y III.A
ApplicationStack(
    app,
    f"{PROJECT_NAME}-ApplicationStack",
    bucket=storage.bucket,
    project_name=PROJECT_NAME,
    env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION),
)

app.synth()
