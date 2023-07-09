import json
import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
import datetime
from datetime import datetime, timedelta, timezone
import awswrangler as wr


dynamodb = boto3.resource('dynamodb')
table_name = 'z'
table = dynamodb.Table(table_name)
dynamodb_client = boto3.client('dynamodb', 'y')
dynamodb = boto3.resource('dynamodb')
genesys_db = dynamodb.Table('y')


current_day = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
current_day_year = current_day[0:4]
current_day_month = current_day[4:6]
current_day_day = current_day[6:]
date_string = "{}-{}-{}".format(current_day_year, current_day_month, current_day_day)
yesterday_str=str(date_string)
print(yesterday_str)
date_string1 = "dt={}-{}-{}".format(current_day_year, current_day_month, current_day_day)
date_string2 = "{}{}{}".format(current_day_year, current_day_month, current_day_day)


def lambda_handler(event, context):
    response = table.scan()
    items = response['Items']
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    
    # Filter for items with yesterday's date
    # filtered_items = [item for item in items if item['Date'] == yesterday_str]
    filtered_items = [item for item in items if yesterday_str in item['Date']]

    
    yyyyy = [item['y'] for item in filtered_items]
    print(yyyyy)
    queue = [item['queue'] for item in filtered_items]
    print(queue)
    Date = [item['Date'] for item in filtered_items]
    print(Date)
    date = [item['Date'].split()[0] for item in filtered_items]
    print(date)
    
    # Read the CSV file from the S3 bucket
    object_key = "{}/yyyyy_intr_dtl_{}.csv".format(date_string1, date_string2)
    path = "s3://yyyyyyyy/bigdata/shared/yyyyy/intr_dtl/" + object_key
    df_dtl = pd.read_csv(path, keep_default_na=False, index_col=False,low_memory=False)

    
    # Convert the "INTERACTION_PHONE_NO" column to string
    df_dtl["INTERACTION_PHONE_NO"] = df_dtl["INTERACTION_PHONE_NO"].astype(str)
    df_dtl["INTERACTION_START_TIME"] = pd.to_datetime(df_dtl["INTERACTION_START_TIME"])
    df_dtl["date"] = df_dtl["INTERACTION_START_TIME"].dt.date
    df_dtl["date"] =df_dtl["date"].astype(str)
    # Extract rows from df_dtl based on 'yyyyy' and 'queue' values
    filtered_df = df_dtl[(df_dtl["INTERACTION_PHONE_NO"].isin(yyyyy)) & (df_dtl["INTERACTION_AGENT_SKILLGROUP"].isin(queue)) & (df_dtl["date"].isin(date))]
    # print(filtered_df)
    
    # # Rename the column
    filtered_df = filtered_df.rename(columns={"INTERACTION_PHONE_NO": "yyyyy","INTERACTION_AGENT_SKILLGROUP":"queue"})
    # print(filtered_df.columns)
    filtered_df["INTERACTION_START_TIME"] = filtered_df["INTERACTION_START_TIME"].astype(str)
    filtered_df["INTERACTION_END_TIME"] = filtered_df["INTERACTION_END_TIME"].astype(str)
    
    
    df = filtered_df[["yyyyy","queue","INTERACTION_ID","INTERACTION_START_TIME","INTERACTION_END_TIME","INTERACTION_CHANNEL","INTERACTION_AGENT_NAME","SOURCE_SYSTEM_NAME"]].copy()
    # print(df.columns)
    print('To address:', df["yyyyy"])
    path=r"yyyyyyyyyyyyfile.csv"
    df.to_csv(path)
    
    
    
    for index, row in df.iterrows():
        partition_key = row['yyyyy']
        # using query method instead of .scan for increasing efficiency
        query = 'SELECT * FROM "kk-yyyyy-join-db" WHERE yyyyy = {}'.format(f"'{partition_key}'")
        customer_response = dynamodb_client.execute_statement(Statement=query, ConsistentRead=True)
        
        print('customer_response:', customer_response)
        
        for i in customer_response['Items']:
            
            if yesterday_str in i['Date']['S']:
                sort_key = i['Date']['S']
        print('sort_key:', sort_key)
        
        id = row['INTERACTION_ID']
        start_time = row['INTERACTION_START_TIME']
        end_time = row['INTERACTION_END_TIME']
        channel = row['INTERACTION_CHANNEL']
        agent_name = row['INTERACTION_AGENT_NAME']
        source_system = row['SOURCE_SYSTEM_NAME']
        
        print('column values:', id,start_time,end_time,channel,agent_name, source_system,partition_key,sort_key)
        
        response = yyyyy_db.update_item(Key={'yyyyy': partition_key, 'Date': sort_key},
                                                    UpdateExpression = "SET INTERACTION_ID = :val1, INTERACTION_START_TIME = :val2, INTERACTION_END_TIME = :val3, INTERACTION_CHANNEL = :val4, INTERACTION_AGENT_NAME = :val5, SOURCE_SYSTEM_NAME = :val6",
                                                    ExpressionAttributeValues = {':val1': id, ':val2': start_time, ':val3': end_time, ':val4': channel, ':val5': agent_name, ':val6': source_system})




