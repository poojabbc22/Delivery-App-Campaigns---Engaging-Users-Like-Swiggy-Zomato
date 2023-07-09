import json
import boto3
import pandas as pd
import datetime
from datetime import datetime, timedelta, timezone
import awswrangler as wr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('xxxxxx')

def lambda_handler(event, context):
    print(event)
    # Retrieve event parameters
    event_body = event['body']
    toAddress = event_body['xxx']
    fromAddress = event_body['x']
    templateId = event_body['x']
    market = event_body['x']
    queue = event_body['x']
    Date = event['timestamp']
    campaign_name = event['x']
    

    response = table.scan(
        FilterExpression='campaign_name = :cname',
        ExpressionAttributeValues={
            ':cname': campaign_name
        }
    )
    
    if response.get('Items'):

        max_sequence_number = max(int(item.get('sequence', '0')) for item in response['Items'])
        new_sequence_number = max_sequence_number + 1
        print("updated seq")
    else:
       
        new_sequence_number = 1
        print("new seq")
    
   
    item = {
        'x': toAddress,
        'x': fromAddress,
        'templateId': templateId,
        'Date': Date,
        'x': market,
        'queue': queue,
        'x': campaign_name,
        'sequence': str(new_sequence_number)
    }
    

    table.put_item(Item=item)
    
    print("Outbound response successfully loaded into xxxxxxxxdb ")
