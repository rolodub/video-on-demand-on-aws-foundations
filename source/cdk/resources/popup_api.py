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
import decimal
# DYNAMO Global Vars
DYNAMO_CONFIG = Config(connect_timeout=0.250, read_timeout=0.250, retries={'max_attempts': 1})
DYNAMO_RESOURCE = boto3.resource('dynamodb', config=DYNAMO_CONFIG)
DYNAMODB_TABLE_NAME = 'not-set'
#DYNAMO_INDEX = "requestor_id-timestamp_created-index"
s3_client = boto3.client('s3')
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

def replace_decimals(obj):
    if isinstance(obj, list):
        for i in range(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
            #return time.strftime('%H:%M:%S', time.gmtime(obj))
        else:
            return float(obj)
    else:
        return obj

def get_items(event,item_type,table):
    #table = DYNAMO_RESOURCE.Table(DYNAMODB_TABLE_NAME)
    pprint(DYNAMODB_TABLE_NAME)
    d_response = []
    if event['queryStringParameters']:
        if event['queryStringParameters']['item_id']:
            item_id = event['queryStringParameters']['item_id']
            d_response = table.query(KeyConditionExpression=Key('item_id').eq(item_id),ScanIndexForward=False)
    else:
        d_response = table.query(IndexName='type_date_index',KeyConditionExpression=Key('item_type').eq(item_type),ScanIndexForward=False)
    pprint(d_response['Items'])
    message = replace_decimals(d_response)
    status_code = 200
    message['state'] = 'success'
    return message

def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        #logging.error(e)
        return None
    # The response contains the presigned URL
    return response

def handler(event, context):
    DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'not-set')
    write_log("INFO", "event: {}".format(json.dumps(event)))
    status_code = 500
    message = {
        'state': 'fail'
    }
    if DYNAMODB_TABLE_NAME == 'not-set':
        write_log("ERROR", "Environment Variable DYNAMODB_TABLE_NAME is not set")
        status_code = 200
        message['state'] = 'broke'

    if event['path'] == '/endpoints':
        if event['httpMethod'] == 'POST':
            try:
                if 'body' in event:
                    response = json.loads(event['body'])
                    if 'item_type' in response:
                        if response['item_type'] == 'endpoint':
                            table = DYNAMO_RESOURCE.Table(DYNAMODB_TABLE_NAME)
                            write_log("INFO",table)
                            item = {
                                "item_id": str(response['item_id']),
                                "item_type": str(response['item_type']), 
                                "item_name": str(response['item_name']),
                                "urls": response['urls'],
                                "item_creation_date": int(time.time()),
                                "timestamp_ttl": int(time.time()) + 3600 # 1hr
                                }
                            d_response = table.put_item(Item=item)
                            write_log("INFO", "DynamoDB Succesful: {}".format(d_response))
                            status_code = 200
                            message['state'] = 'success'
                    else:
                        write_log("WARNING", "type not implemented, doing nothing")
            except:
                status_code = 200
                message['state'] = 'broke'

        elif event['httpMethod'] == 'GET':
            item_type = "endpoint"
            table = DYNAMO_RESOURCE.Table(DYNAMODB_TABLE_NAME)
            d_response = []
            if event['queryStringParameters']:
                if event['queryStringParameters']['item_id']:
                    item_id = event['queryStringParameters']['item_id']
                    d_response = table.query(KeyConditionExpression=Key('item_id').eq(item_id),ScanIndexForward=False)
            else:
                d_response = table.query(IndexName='type_date_index',KeyConditionExpression=Key('item_type').eq(item_type),ScanIndexForward=False)
            pprint(d_response['Items'])
            message = replace_decimals(d_response)
            status_code = 200
            message['state'] = 'success'
        elif event['httpMethod'] == 'DELETE':
            item_type = "endpoint"
            table = DYNAMO_RESOURCE.Table(DYNAMODB_TABLE_NAME)
            d_response = []
            if event['queryStringParameters']:
                if event['queryStringParameters']['item_id']:
                    item_id = event['queryStringParameters']['item_id']
                    d_response = table.delete_item(Key={'item_id':item_id})
            else:
                message['state'] = 'not found'
            status_code = 200
            message['state'] = 'success'
            
    elif event['path'] == '/in_srt':
        if event['httpMethod'] == 'GET':
            pass

    elif event['path'] == '/stock_medias':
        if event['httpMethod'] == 'GET':
            item_type = "stock_media"
            table = DYNAMO_RESOURCE.Table(DYNAMODB_TABLE_NAME)
            d_response = []
            if event['queryStringParameters']:
                #if event['queryStringParameters']['item_id']:
                try:
                    item_id = event['queryStringParameters']['item_id']
                    d_response = table.query(KeyConditionExpression=Key('item_id').eq(item_id),ScanIndexForward=False)
                except:
                    pass
                #elif event['queryStringParameters']['item_name']:
                try:
                    item_name = event['queryStringParameters']['item_name']
                    d_response = table.query(IndexName='name_date_index',KeyConditionExpression=Key('item_name').eq(item_name),ScanIndexForward=False)
                except:
                    pass
                #elif event['queryStringParameters']['item_category']:
                try:
                    item_category = event['queryStringParameters']['item_category']
                    d_response = table.query(IndexName='item_category-index',KeyConditionExpression=Key('item_category').eq(item_category),ScanIndexForward=False)
                except:
                    pass
            else:
                d_response = table.query(IndexName='type_date_index',KeyConditionExpression=Key('item_type').eq(item_type),ScanIndexForward=False)
            pprint(d_response['Items'])
            message = replace_decimals(d_response)
            status_code = 200
            message['state'] = 'success'
    elif event['path'] == '/copy_object_to_bucket':
        copy_source = {
            'Bucket': 'mybucket',
            'Key': 'mykey'
            }
        bucket = s3_client.Bucket('otherbucket')
        bucket.copy(copy_source, 'otherkey')

   ## GET - ivs stream recordings
    elif event['path'] == '/ivs-stream-recs':
        pass
    return {
        'statusCode': 200,
        'body': json.dumps(message),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
            },
        }