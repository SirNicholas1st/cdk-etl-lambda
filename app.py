#!/usr/bin/env python3

import aws_cdk as cdk

from cdk_etl_lambda.cdk_etl_lambda_stack import CdkEtlLambdaStack


app = cdk.App()
CdkEtlLambdaStack(app, "CdkEtlLambdaStack")

app.synth()
