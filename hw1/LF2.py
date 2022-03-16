import json
import boto3
import random
import requests
from botocore.exceptions import ClientError
from requests.auth import HTTPBasicAuth

def format_message(top3, cuisine, people, date, time):
    message = "Hello! Here are my " + cuisine + " restaurant suggestions for " + people + " people, for " \
                + date + " at " + time + ": "
    db = boto3.resource("dynamodb")
    table = db.Table("yelp-restaurants")
    cnt = 0
    for id in top3:
        cnt += 1
        response = table.get_item(Key={"Business_ID": id})["Item"]
        name, address = response["Name"], response["Address"]
        message += str(cnt) + ". " + name + ", located at " + address
        if cnt < 3:
            message += ", "
        else:
            message += ". "
    message += "Enjoy your meal!"
    return message

def send_message(message, email):
    sender = "Jincheng Xu <jx2467@columbia.edu>"
    recipient = email
    subject = "COMSE6998 Assignment1 Test Email"
    text = message
    charset = "UTF-8"
    client = boto3.client("ses", "us-east-1")
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': charset,
                        'Data': text,
                    },
                },
                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                },
            },
            Source=sender
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def lambda_handler(event, context):
    # TODO implement
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.us-east-1.amazonaws.com/642527119266/OrderQueue"
    response = sqs.receive_message(
            QueueUrl = queue_url,
            MaxNumberOfMessages = 1,
            MessageAttributeNames = [
                'All'
            ],
    )
    # unreliable sqs receive, ignore it
    if "Messages" not in response:
        return {
            "statusCode": 200,
            "body": json.dumps("Not receiving from sqs!")
        }
    
    # get the message and delete it from queue
    message = response["Messages"][0]
    receipt_handle = message['ReceiptHandle']
    sqs.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )
    
    message_ls = message["Body"].split(",")
    location, cuisine, people, date, time, email = message_ls[0], message_ls[1], message_ls[2], message_ls[3], message_ls[4], message_ls[5]
    
    region = "us-east-1"
    service = "es"
    awsauth = HTTPBasicAuth("xujincheng", "7Senses_kiki")
    es_url = "https://search-elasticsearch-rbcr4qxzwbte3wt4giculqncli.us-east-1.es.amazonaws.com"
    url = es_url + "/_search?q=" + cuisine
    res = requests.get(url, auth=awsauth).json()

    results = set()
    for restaurant in res["hits"]["hits"]:
        results.add(restaurant["_source"]["Business_ID"])
    results = list(results)
    random.shuffle(results)
    top3 = results[:3]
    
    message = format_message(top3, cuisine, people, date, time)
    send_message(message, email)
    
    return {
        "statusCode": 200,
        "body": json.dumps("Execute successfully!")
    }