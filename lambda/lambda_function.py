import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):

    print(event)
    return {
        "Status": 200,
        "Body": "Success"
    }