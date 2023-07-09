import boto3
import json
# this is too update the status whether for a particular person the campign data has been sent, if it has been sent then we will change the status
#to do which denotes Deeleted
def lambda_handler(event, context):
    print(event)
    api_response=event['body-json']
    ani = api_response[y']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('y')

    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Key('y').eq(y)
    )

    items = response['Items']
    if len(items) > 0:
        item = items[0]
        item['Record_Status'] = 'D'
        table.put_item(Item=item)
        print(f"Record status for {phno} has been updated.")
        return {
            'statusCode': 200,
            'body': json.dumps('Hello record status updated!')
        }
    else:
        print(f"Requested  {phno} is not present in the database.")
        return {
            'statusCode': 200,
            'body': json.dumps('sorry! the phno is not there')
        }

