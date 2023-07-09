import pandas as pd
import boto3
from datetime import datetime, date, timedelta

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('x')

current_day = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
current_day_year = current_day[0:4]
current_day_month = current_day[4:6]
current_day_day = current_day[6:]

date_string = "{}{}{}".format(current_day_year, current_day_month, current_day_day)



def lambda_handler(event, context):
    response = table.scan()
    items = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    df = pd.DataFrame(items)
    df['Date'] = pd.to_datetime(df['Date'])

    # Filter based on yesterday
    yesterday = date.today() - timedelta(days=1)
    yesterday_filter = df['Date'].dt.date.astype(str) == str(yesterday)
    filtered_df = df[yesterday_filter]

    print(filtered_df)
    for market in filtered_df['market']:
        if market == 'x':
            bucket_name = 's3://x'
        elif market == 'x':
            bucket_name = 's3://x'
        elif market == 'BRB':
            bucket_name = 's3://x'
        elif market == 'JAM':
            bucket_name = 's3://x'
        elif market == 'x':
            bucket_name = 's3://x'
        elif market == 'x':
            bucket_name = 's3://x'
        elif market == 'x':
            bucket_name = 'x'
        else:
            print("Invalid Market Found")
           

        file_path = f"{bucket_name}/{current_day_year}/{current_day_month}/{current_day_day}/x_Campaign_Report-{date_string}.csv"
        print(file_path)
        filtered_df.to_csv(file_path, index=False)
        print("File written")

        # # Delete data from DynamoDB based on partitionKey and sortKey
         for _, row in filtered_df.iterrows():
             toAddress = row['x']
             current_timestamp = str(row['Date'])  # Convert to string
             table.delete_item(
                 Key={
                     'toAddress': toAddress,
                     'current_timestamp': Date
                }
             )

         print("Data deleted from DynamoDB")
