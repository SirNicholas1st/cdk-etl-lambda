import json
import boto3
import unittest
import pandas as pd
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
        self.s3_client = boto3.client("s3", region_name=self.test_region)
        self.s3_client.create_bucket(Bucket=self.bucket_name)

        # setting up a sns
        self.sns_topic_name = "test_topic"
        self.sns = boto3.resource("sns", region_name=self.test_region)
        self.sns_topic = self.sns.create_topic(Name=self.sns_topic_name)

        # setting up sqs
        self.sqs_que_url = "test-sqs"
        self.sqs = boto3.client("sqs", region_name=self.test_region)
        self.que_url = self.sqs_que_url

        # simulating a file upload to s3. I want to have 2 files, since a SQS event can contain multiple object created events.
        self.file_contents = ["Hello, World1", "Hello;World;2"]
        self.s3_object_keys = ["test_file1.txt", "test_file2.csv"]

        # empty list for storing the sns messages, will be used to create a sqs event
        self.sns_messages = []
        # looping the sample files "uploading" them to s3 and publishing a mock up message to the sns
        for key, content in zip(self.s3_object_keys, self.file_contents):

            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=content)

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
        Testing the extract_file_info function from the lambda. The purpose of the function is to extract the source buckets and file keys from the sqs event.
        In this test we use the mocked up version of the sqs event.
        The loop is need since the lambda handler does the looping.
        This function might not need testing as it is now since it just uses the keys from the event to get the good stuff, but good practise.
        """
        
        # yea this is kinda stupid, maybe i should have done the extract file info so that it did the looping. Well next time then.
        for record in self.sqs_event["Records"]:
            record_body = json.loads(record["body"])
            for item in record_body["Records"]:
                
                # storing the output of the function
                self.file_info = lambda_function.extract_file_info(item)

                # hard coded keys ok, since im betting the the sqs structure doesnt change anywhile soon
                expected_file_info = {
                    "source_bucket": item["s3"]["bucket"]["name"],
                    "file_name": item["s3"]["object"]["key"]
                }

                self.assertEqual(self.file_info, expected_file_info)

    def test_read_file_to_df(self):
        """
        Testing the function that reads the file into a DF. 
        """

        # simulating the result of the extract file info to a variable
        # manually taking the sqs item for the txt file and feeding it to the extract function
        sqs_item_for_test_txt = json.loads(self.sqs_event["Records"][0]["body"])["Records"][0]
        file_info_test_txt = lambda_function.extract_file_info(sqs_item_for_test_txt)
        
        # here we test the function with a txt file, we are supposed to get a ValueError.
        expected_error = ValueError
        
        with self.assertRaises(expected_exception=expected_error):
            output_txt_file = lambda_function.read_csv_to_df(file_info=file_info_test_txt, s3=self.s3_client)
        
            
        # Now we test it with a csv.
        sqs_item_for_test_csv = json.loads(self.sqs_event["Records"][1]["body"])["Records"][0]
        file_info_test_csv = lambda_function.extract_file_info(sqs_item_for_test_csv)
        expectected_result = pd.DataFrame(columns=["Hello", "World", "2"])
        
        actual_result = lambda_function.read_csv_to_df(file_info=file_info_test_csv, s3=self.s3_client)
        
        pd.testing.assert_frame_equal(expectected_result, actual_result)



if __name__ == "__main__":
    unittest.main()