#!/usr/bin/env python
import json
import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from botocore.client import Config
import uuid
import json
from pprint import pprint
import time
from pathlib import Path
from decimal import Decimal

def replace_floats(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_floats(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_floats(obj[k])
        return obj
    elif isinstance(obj, float):
        return str(obj)

    else:
        return obj

# DYNAMO Global Vars
DYNAMO_CONFIG = Config(connect_timeout=0.250, read_timeout=0.250, retries={'max_attempts': 1})
DYNAMO_RESOURCE = boto3.resource('dynamodb', config=DYNAMO_CONFIG)

DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'not-set')
S3_DEST = os.environ.get('S3_DEST')
table = DYNAMO_RESOURCE.Table(DYNAMODB_TABLE_NAME)
#DYNAMO_INDEX = "requestor_id-timestamp_created-index"
DEBUG_LEVEL = "INFO"
def write_log(log_level, log_string):
    log_label = {
        "OFF"   : 0,
        "ERROR" : 1,
        "WARN"  : 2,
        "INFO"  : 3,
    }
    if log_label[log_level] <= log_label[DEBUG_LEVEL]:
        print("{}: {}".format(log_level,log_string))

def handler(event, context):
    if event['Records']:
        for record in event['Records']:
            message = json.loads(event['Records'][0]['Sns']['Message'])
            item = {
                        "item_id": str(message['Id']),
                        "item_type": 'stock_media', 
                        "item_name": Path(message['InputFile']).stem,
                        "item_category":"not set",
                        "urls":[{
                                "url_type":"media_master",
                                "url_value":message['InputFile']
                            },
                            {
                                "url_type":"media_hls",
                                "url_value":str(message['Outputs']['HLS_GROUP'][0])
                            },
                            {
                                "url_type":"bucket_dest_arn",
                                "url_value":S3_DEST
                            }],
                        "inputDetails":message['InputDetails'],
                        "item_creation_date": int(time.time()),
                        "timestamp_ttl": int(time.time()) + 3600 # 1hr
                        }
            print(item)
            d_response = table.put_item(Item=replace_floats(item))
            
            write_log("INFO", "DynamoDB Succesful: {}".format(d_response))
            #status_code = 200
            #message['state'] = 'success'
    else:
        write_log("WARNING", "error S3 in dynamodb, doing nothing")
