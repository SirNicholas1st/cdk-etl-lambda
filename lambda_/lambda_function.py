import os
import json
import gzip
import boto3
import logging
import pandas as pd
from io import BytesIO, StringIO
from datetime import datetime as dt

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TARGET_BUCKET = os.getenv("TARGET_BUCKET_NAME")

def extract_file_info(item: json) -> dict:

    source_bucket = item["s3"]["bucket"]["name"]
    file_name = item["s3"]["object"]["key"]

    return {
        "source_bucket": source_bucket,
        "file_name": file_name
    }

def read_csv_to_df(file_info: dict, s3: any) -> pd.DataFrame:

    bucket = file_info["source_bucket"]
    file_name = file_info["file_name"]
    s3_object = s3.get_object(Bucket=bucket, Key=file_name)

    # just for the sake of practising writing tests.
    if not file_name.endswith(".csv"):
         raise ValueError

    file_string = s3_object["Body"].read().decode("utf-8")

    csv_df = pd.read_csv(StringIO(file_string), delimiter=";")

    return csv_df

def zip_csv(data: bytes) -> bytes:
     # compressing the csv data, this function is used in the create_csv_from_df function

    # creating a in memory buffer with BytesIO, in a sense it allows us to write to a "file" without actually writing to disk.
    with BytesIO() as compressed_data:
        # creating an gzip fileobject and writing the data to the BytesIO buffer.
        with gzip.GzipFile(fileobj=compressed_data, mode="wb") as f:
               f.write(data)
        
        # we return the contents of the buffer as bytes.
        return compressed_data.getvalue()

def create_csv_from_df(csv_df: pd.DataFrame) -> bytes:
     # creating a csv and zipping its contents with the zip_csv, we return the bytes for the file

     file_bytes = csv_df.to_csv(index=False, sep=";").encode("utf-8")
     compressed_csv_bytes = zip_csv(data = file_bytes)
     return compressed_csv_bytes

def upload_to_target_bucket(compressed_bytes: bytes, target_bucket: str, s3: any) -> None:
     
     unique_identifier = dt.now().strftime(format="%Y%m%d%m%f")
     file_name = f"test_csv_{unique_identifier}.csv.gz"

    # storing the csv to the target bucket, if prefix is needed, include it in the key.
     s3.put_object(Body=compressed_bytes, Bucket=target_bucket, Key=file_name)

     return None
     
     

def lambda_handler(event, context):

    
    s3 = boto3.client("s3")

    # if one message from the sqs event fails we want to reprocess it
    # https://docs.aws.amazon.com/lambda/latest/dg/example_serverless_SQS_Lambda_batch_item_failures_section.html
    batch_item_failures = []
    sqs_batch_response = {}

    for record in event["Records"]:
            record_body = json.loads(record["body"])

            for item in record_body["Records"]:

                try:
                    file_info = extract_file_info(item)
                    csv_df = read_csv_to_df(file_info=file_info, s3=s3)
                    compressed_file_bytes = create_csv_from_df(csv_df=csv_df)
                    upload_to_target_bucket(compressed_bytes=compressed_file_bytes, target_bucket=TARGET_BUCKET, s3=s3)
                    
                except Exception as e:
                    logger.info(f"""Got an error: {e} while processing the following event record {record}.
                                Continuing to next iteration and appending this one to batch item failures for reprocessing""")
                    batch_item_failures.append({"itemIdentifier": record['messageId']})
                    continue

    sqs_batch_response["batchItemFailures"] = batch_item_failures

    return sqs_batch_response
    
    