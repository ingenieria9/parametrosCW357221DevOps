#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.lambda_api_stack import LambdaApiStack

app = cdk.App()
LambdaApiStack(app, "LambdaApiStack", env=cdk.Environment(region="us-west-2"))
app.synth()
