from constructs import Construct
import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_lambda as lambda_,
    aws_s3 as s3,
    RemovalPolicy,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as lambda_events
)


class CdkEtlLambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        # this is one example how to use variables in CDK. The variable is passed to the cdk synth/command
        # example cdk synth --parameters ENVIRONMENT=asdasdaads
        environment = cdk.CfnParameter(self, "ENVIRONMENT",
                                       type="String",
                                       description="this parameter will be used in naming resources.").value_as_string

        # Creating a deadletter queue, in prod use, the retention period should be longer so you have more time to deal with any failures.
        dlq = sqs.Queue(
            self, "CdkEtlLambdaDLQ",
            retention_period=Duration.days(1)
        )

        dead_letter_queue = sqs.DeadLetterQueue(
            queue=dlq,
            # if a message fails, we try it 2 more times, then we move it to the DLQ.
            max_receive_count=3
        )

        # This queue will receive the messages from the SNS topic and feed them to the Lambda.
        upload_queue = sqs.Queue(
            self, "CdkEtlLambdaQueue",
            # if the SQS event fails for somereason we wait 30s and let the lambda try again.
            visibility_timeout=Duration.seconds(30),
            # this option will help to lower costs since the amount of empty receives is lower, but this also slows down the pipe.
            receive_message_wait_time=Duration.seconds(10),
            dead_letter_queue=dead_letter_queue
        )

        # defining a SNS topic
        topic = sns.Topic(
            self, "CdkEtlLambdaTopic"
        )

        # defining a SQS subscription
        sqs_subscription = subs.SqsSubscription(
            upload_queue,
            raw_message_delivery=True
        )

        # adding the SQS subscription to the SNS queue.
        topic.add_subscription(sqs_subscription)

        # lifecycle rule for the bucket not needed but helps with costs.
        # This bucket is the landing bucket for the files and when an object is created here it should send a message to the SNS
        source_bucket = s3.Bucket(
            self, "cdkEtlSourceBucket",
            bucket_name=f"{environment}-cdk-source-bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            # this removal policy means, that if the stack is deleted the bucket will also be deleted.
            # if the bucket is not empty it will raise an error and the stack will remain in CloudFormation with only the resources that it failed to delete.
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    enabled=True,
                    expiration=Duration.days(1)
                )  
            ]
        )

        # here we add an event notification for every type of object created to the source bucket
        # when an object is created it will send a message to the SNS-queue
        source_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.SnsDestination(topic))

        # This is the bucket where the Lambda saves the parsed files.
        target_bucket = s3.Bucket(
            self, "cdkEtlTargetBucket",
            bucket_name=f"{environment}-cdk-target-bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    enabled=True,
                    expiration=Duration.days(1)
                )
            ]
        )

        # creating a pandas layer for the lambda
        layer = lambda_.LayerVersion(self, "pandas_layer",
                                     code=lambda_.Code.from_asset("layers/pandas.zip"),
                                     description="A pandas layer for the lambda",
                                     compatible_runtimes=[
                                         lambda_.Runtime.PYTHON_3_12,
                                         lambda_.Runtime.PYTHON_3_11,
                                         lambda_.Runtime.PYTHON_3_10
                                     ],
                                     # if we delete the stack i want this gone also.
                                     removal_policy=RemovalPolicy.DESTROY
                                     )

        # creating the lambda to the stack.
        etl_lambda = lambda_.Function(
            self, "Function",
            function_name=f"{environment}-cdk-parser-lambda",
            description="The parser lambda.",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(path="lambda"),
            handler="lambda_function.lambda_handler",
            layers=[layer]
        )

        # adding an event source to the Lambda (SQS)
        # we want at the max 10 s3 events at the same time
        # also we will wait at the most 10 seconds before invoking the function.
        etl_lambda.add_event_source(lambda_events.SqsEventSource(upload_queue,
                                                                  batch_size=10,
                                                                  max_batching_window=Duration.seconds(10)))

    


        
