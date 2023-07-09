import json
import boto3
import pandas as pd
import datetime
from dateutil import tz


def process_message(body,market, queue_url, template_file_key, whatsapp_file_key):
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    response_id1 = body['y']
    fromAddress = body['y']
    campaign_name=body['y']
    
    obj = s3.get_object(Bucket='xxxxxxxxxxxx', Key=template_file_key)
    df = pd.read_csv(obj['Body'])
    # Convert column names to lowercase
    df.columns = df.columns.str.lower()
  
    
    obj1 = s3.get_object(Bucket='xxxxxxxxxxx', Key=whatsapp_file_key)
    df1 = pd.read_csv(obj1['Body'])
    # Convert column names to lowercase
    df1.columns = df1.columns.str.lower()
    #numbers of the public to send campign notification
    df1['whatsapp_number'] = df1['whatsapp_number'].astype(str)
    
    filtered_df = df[df['template_name'] == response_id1]
    if not filtered_df.empty:
        template_id = filtered_df['response_id'].values[0]
        body['responseId'] = template_id
    else:
        # Store input data as JSON in S3 with timestamp
        print("No template ID found for", response_id1)
        now = datetime.datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        date_str_millisecz = now.strftime("%Y%m%d%H%M%S%f")[:-3]
        json_data = json.dumps(body)
        #for the regions of carribean islands
        key = f'exception_parameter/{market}/responseId_exception/{date_str}/responseId_input{date_str_millisecz}.json'
        s3.put_object(Body=json_data, Bucket='xxxxxxxx', Key=key)
        return  # Skip sending SQS message and invoking lambda(if whatsapp id or template id doesnt match)
   
    filtered_df1 = df1[df1['whatsapp_number'] == fromAddress]
    if not filtered_df1.empty:
        whatsapp_id = filtered_df1['whatsapp_id'].values[0]
        body['fromAddress'] = whatsapp_id
     
    else:
        # Store input data as JSON in S3 with timestamp
        print("No WhatsApp ID found for", fromAddress)
        now = datetime.datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        date_str_millisecz = now.strftime("%Y%m%d%H%M%S%f")[:-3]
        json_data = json.dumps(body)
        key = f'exception_parameter/{market}/fromAddress_exception/{date_str}//fromAddress_input{date_str_millisecz}.json'
        s3.put_object(Body=json_data, Bucket='xxxxxxx', Key=key)
        return  # Skip sending SQS message and invoking lambda

    print("After Mapping:", body)

    # Process message
    message_body = json.dumps(body)
    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body
    )
    print("sent a message to Responsive SQS")
    
    # Invoke xxxxxxxx lambda
    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lambda_payload = {
        'body': body,
        'timestamp': current_timestamp,
        'campaign_name':campaign_name
    }
    lambda_client.invoke(
        FunctionName='xxxxxxxxx',
        InvocationType='Event',
        Payload=json.dumps(lambda_payload)
    )
    print("Invoked xxxxxxxxx")


    
    ##Invoke to xxxxxxxxxlambda
    phono = body['toAddress']
    zzzz = body['queue']
    kkkk = body['market']
    responseId = body['responseId']
    insert_payload = {'phono': phono, 'zzzz': zzzz, 'kkkk': kkkk, 'responseId': responseId}
    lambda_client.invoke(
        FunctionName='xxxxxxxx',
        InvocationType='Event',
        Payload=json.dumps(insert_payload)
    )
    print("Invoked xxxxxxxxx-lambda for payload")

def lambda_handler(event, context):
    
    # Create SQS and S3 clients
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')
# we got batchwise input data from the SQS
    # Define the queue URLs
    queue_url_xx ='xxxxxxxxxx'
    queue_url_yy =xxxxxxxxx'
    queue_url_yy ='xxxxxxxxx'

    print(event)

    if event:
        messages_to_reprocess = []
        batch_failure_response = {}
        payloads = []
        for record in event["Records"]:
            try:
                body = json.loads(record["body"])
                print("Before Mapping:", body)
              

                if body["market"] == "BRB":
                    process_message(body,'BRB', queue_url_xx, 'xxxxxxxxxxx/TemplateID_file.csv', xxxxxxxxxxxwhatsappID_file.csv')
                elif body["market"] == "BHS":
                    process_message(body,'BHS', queue_url_xx, 'xxxxxx', 'xxxxxxx')
                elif body["market"] == "JAM":
                    process_message(body,'JAM', queue_url_xx, xxxxxxxxxxxTemplateID_file.csv', 'xxxxxxxxxxxxxwhatsappID_file.csv')
                elif body["market"] == "CUW":
                    process_message(body, 'CUW', queue_url_xx, 'xxxxxxxxxxxxxxCuracao/TemplateID_file.csv', 'xxxxxxwhatsappID_file.csv')
                elif body["market"] == "TTO":
                    process_message(body, 'TTO', queue_url_c, 'xxxxxxxxxTemplateID_file.csv', 'xxxxxxxxxxxxxwhatsappID_file.csv')
                elif body["market"] == "yy":
                    process_message(body, 'yy', queue_url_cp, 'xxxxxxxxxxxTemplateID_file.csv', 'xxxxxxxxxxxxxwhatsappID_file.csv')
                elif body["market"] == "yy":
                    process_message(body, 'yy', queue_url_lc, 'xxxxxxxxxxxxeID_file.csv', 'xxxxxxxxxx/whatsappID_file.csv')
                else:
                    print("Country not matched")

            except Exception as e:
                messages_to_reprocess.append({"itemIdentifier": record.get('messageId', '')})
                print("Exception occurred:", e)
# It is a batch processing of messages from SQS
        batch_failure_response["batchItemFailures"] = messages_to_reprocess
        print(batch_failure_response)
        return batch_failure_response



