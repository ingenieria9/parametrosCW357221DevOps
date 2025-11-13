#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.lambda_api_stack import LambdaApiStack
from stacks.s3_storage_stack import StorageStack
from stacks.file_gen_stack import FileGenStack
from stacks.db_access_stack import DbAccessStack
from stacks.lambda_ecr_s3_trigger import LambdaEcrS3TriggerStack
from stacks.arcgis_integration_stack import ArcGISIntStack
from stacks.lambda_layers import LayersStack
from stacks.measurement_integration_stack import MeasurementIntStack
from stacks.file_send_stack import FileSendStack
from stacks.api_gen_stack import ApiGenStack

app = cdk.App()

#Variables que se usaran para identificar los recursos
PROJECT_NAME = "CW357221ParametrosDevOps"


MAIN_REGION = "us-west-2"
ACCOUNT = "339713063336"

#El proyecto usa una lambda de acceso a una DB. La DB existe en otra región,
#por lo que la lambda de acceso a la DB entra como variable de entorno para 
#las lambdas que la invocan


# ------------- Stacks --------------------
# Cada stack representa un conjunto de recursos de AWS 


# Stack de prueba inicial: Api gateway v2 + lambda
#LambdaApiStack(app, "LambdaApiStack", env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION))


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

#Stack layers 
layers_stack = LayersStack(app, f"{PROJECT_NAME}-LayersStack", project_name=PROJECT_NAME, env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION))


#output de stack: Lambda ARN
#db_stack.db_access_lambda_arn


# Stack de almacenamiento S3 (Bucket del proyecto)
storage = StorageStack(app, f"{PROJECT_NAME}-StorageStack", project_name=PROJECT_NAME, env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION))

# Stack para etapa 2 "Generación de entregables" para Fase I, II  y III.A
filegen_stack = FileGenStack(
    app,
    f"{PROJECT_NAME}-FileGenStack",
    bucket=storage.bucket,
    project_name=PROJECT_NAME,
    env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION),
    db_access_lambda_arn=db_stack.db_access_lambda_arn, 
    openpyxl_layer=layers_stack.openpyxl_layer,
    docxtpl_layer=layers_stack.docxtpl_layer,
    requests_layer = layers_stack.requests_layer
)

# Stack conversor docx a pdf (lambda en ECR + trigger S3)
LambdaEcrS3TriggerStack(app, f"{PROJECT_NAME}-LambdaEcrS3TriggerStack",     bucket_name=storage.bucket.bucket_name,
    bucket_arn=storage.bucket.bucket_arn, project_name=PROJECT_NAME, env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION))

# Stack para etapa 3 "integración ArcGIS" 

arcgis_int_Stack = ArcGISIntStack(
    app,  f"{PROJECT_NAME}-ArcGISIntStack",
    bucket_name=storage.bucket.bucket_name,
    bucket_arn=storage.bucket.bucket_arn, 
    project_name=PROJECT_NAME,
    db_access_lambda_arn=db_stack.db_access_lambda_arn,
    entregables_fase_x=[
        filegen_stack.entregable_fase1_lambda.function_arn
    ],
    request_layer = layers_stack.requests_layer,
    env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION), 
)

arcgis_int_Stack.add_dependency(filegen_stack)



measurement_int_Stack = MeasurementIntStack(
    app,  f"{PROJECT_NAME}-MeasurementIntStack",
    bucket_name=storage.bucket.bucket_name,
    bucket_arn=storage.bucket.bucket_arn, 
    project_name=PROJECT_NAME,
    drive_layer = layers_stack.google_layer,
    db_access_lambda_arn = db_stack.db_access_lambda_arn,
    env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION), 
)

file_send_stack = FileSendStack(
    app,
    f"{PROJECT_NAME}-FileSendStack",
    bucket_name=storage.bucket.bucket_name,
    bucket_arn=storage.bucket.bucket_arn,
    project_name=PROJECT_NAME,
    db_access_lambda_arn=db_stack.db_access_lambda_arn,
    env=cdk.Environment(account=ACCOUNT, region=MAIN_REGION),
    request_layer = layers_stack.requests_layer,
)

ApiGenStack(
    app, f"{PROJECT_NAME}-ApiGenStack", 
    lambda_changes = arcgis_int_Stack.changes_lambda.function_arn, lambda_sendFile=file_send_stack.send_file_lambda.function_arn)


app.synth()
