#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.lambda_api_stack import LambdaApiStack
from stacks.s3_storage_stack import StorageStack
from stacks.application_stack import ApplicationStack

app = cdk.App()

PROJECT_NAME = "CW357221ParametrosDevOps"
ENV_NAME = "dev"
PREFIX = f"{PROJECT_NAME}{ENV_NAME}"


LambdaApiStack(app, "LambdaApiStack", env=cdk.Environment(region="us-west-2"))
storage = StorageStack(app, f"{PROJECT_NAME}-StorageStack", project_name=PROJECT_NAME)

ApplicationStack(
    app,
    f"{PROJECT_NAME}-ApplicationStack",
    bucket=storage.bucket,
    project_name=PROJECT_NAME,
    env=cdk.Environment(region="us-west-2"),
)

app.synth()
