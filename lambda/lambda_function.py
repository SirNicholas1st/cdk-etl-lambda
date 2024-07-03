import json
import logging
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def extract_file_info(item: json) -> dict:

    source_bucket = item["s3"]["bucket"]["name"]
    file_name = item["s3"]["object"]["key"]

    return {
        "source_bucket": source_bucket,
        "file_name": file_name
    }

def lambda_handler(event, context):

    # if one message from the sqs event fails we want to reprocess it
    # https://docs.aws.amazon.com/lambda/latest/dg/example_serverless_SQS_Lambda_batch_item_failures_section.html
    batch_item_failures = []
    sqs_batch_response = {}

    for record in event["Records"]:
            record_body = json.loads(record["body"])
            for item in record_body["Records"]:
                try:
                    file_info = extract_file_info(item)
                    print(file_info)
                except Exception as e:
                    logger.info(f"""Got an error: {e} while processing the following event record {record}.
                                Continuing to next iteration and appending this one to batch item failures for reprocessing""")
                    batch_item_failures.append({"itemIdentifier": record['messageId']})
                    continue

    sqs_batch_response["batchItemFailures"] = batch_item_failures

    return sqs_batch_response
    
    