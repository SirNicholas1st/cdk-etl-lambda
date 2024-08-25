import json
import boto3
import unittest
from moto import mock_aws
from moto.sns import sns_backends
from moto.core import DEFAULT_ACCOUNT_ID
from cdk_etl_lambda.lambda_ import lambda_function


@mock_aws
class TestClass(unittest.TestCase):

    def setUp(self) -> None:
        """
        Setting up needed mock resources for the tests
        and simulating the dataflow s3 --> sns --> sqs. Not necessary but good practise and reminder on sns and sqs structures.
        """

        self.test_region = "us-east-1"
        # setting up a mock s3, somehow moto is super dead set on wanting the region as us-east-1
        self.bucket_name = "test-bucket"
        self.s3_conn = boto3.resource("s3", region_name=self.test_region)
        self.s3_conn.create_bucket(Bucket=self.bucket_name)

        # setting up a sns
        self.sns_topic_name = "test_topic"
        self.sns = boto3.resource("sns", region_name=self.test_region)
        self.sns_topic = self.sns.create_topic(Name=self.sns_topic_name)

        # setting up sqs
        self.sqs_que_url = "test-sqs"
        self.sqs = boto3.client("sqs", region_name=self.test_region)
        self.que_url = self.sqs_que_url

        # simulating a file upload to s3. I want to have 2 files, since a SQS event can contain multiple object created events.
        self.file_contents = ["Hello, World1", "Hello, World2!"]
        self.s3_object_keys = ["test_file1.txt", "test_file2.txt"]

        # empty list for storing the sns messages, will be used to create a sqs event
        self.sns_messages = []
        # looping the sample files "uploading" them to s3 and publishing a mock up message to the sns
        for key, content in zip(self.s3_object_keys, self.file_contents):

            self.s3_conn.Bucket(self.bucket_name).put_object(Key=key, Body=content)

        # simulating s3 sending a message to the sns que, seems like there is no way to automate this based on the object created.
            s3_event = {
                        "Records": [{
                            "eventVersion": "2.1",
                            "eventSource": "aws:s3",
                            "awsRegion": "us-east-1",
                            "eventTime": "2024-08-25T12:00:00.000Z",
                            "eventName": "ObjectCreated:Put",
                            "s3": {
                                "bucket": {
                                    "name": self.bucket_name
                                },
                                "object": {
                                    "key": key
                                }
                            }
                        }]
                    }
            
            # publishing the s3 event to sns
            sns_message = json.dumps(s3_event)
            self.sns_topic.publish(Message=sns_message)

            # storing the message for sqs use
            self.sns_messages.append(sns_message)

        # creating a mock up sqs event
        self.sqs_event = {
            "Records":[{"body": message} for message in self.sns_messages]
        }

        
        return super().setUp()

        
    def test_extract_file_info(self):
        """
        Testing the extract_file_info function from the lambda.
        In this test we use the mocked up version of the sqs event.
        The loop is need since the lambda handler does the looping.
        This function might not need testing as it is now since it just uses the keys from the event to get the good stuff, but good practise.
        """
        
        # yea this is kinda stupid, maybe i should have done the extract file info so that it did the looping. Well next time then.
        for record in self.sqs_event["Records"]:
            record_body = json.loads(record["body"])
            for item in record_body["Records"]:
                
                # storing the output of the function
                file_info = lambda_function.extract_file_info(item)

                # hard coded keys ok, since im betting the the sqs structure doesnt change anywhile soon
                expected_file_info = {
                    "source_bucket": item["s3"]["bucket"]["name"],
                    "file_name": item["s3"]["object"]["key"]
                }

                self.assertEqual(file_info, expected_file_info)

    # TODO more tests
            

if __name__ == "__main__":
    unittest.main()