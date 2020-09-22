# LF2

import boto3
import json
import logging
from boto3.dynamodb.conditions import Key, Attr
from botocore.vendored import requests
from botocore.exceptions import ClientError
from boto3 import resource
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests
import random

REGION = 'us-east-1'
HOST = 'https://search-restaurants-6awrywmeqz2cwwks2t4vk3m6ka.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def getSQSMsg():
    SQS = boto3.client("sqs")
    url = 'https://sqs.us-east-1.amazonaws.com/999183881955/DiningConciergeSQS'
    response = SQS.receive_message(
        QueueUrl=url, 
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    
    print('XYZ - response from SQS', response)
    try:
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    print('XYZ - message', message)
    SQS.delete_message(
            QueueUrl=url,
            ReceiptHandle=message['ReceiptHandle']
        )
    logger.debug('Received and deleted message: %s' % response)
    return message

def lambda_handler(event, context):
    message = getSQSMsg() #data will be a json object
    if message is None:
        return
    
    print('XYZ - message:', message)
    cuisine = message["MessageAttributes"]["Cuisine"]["StringValue"]
    location = message["MessageAttributes"]["Location"]["StringValue"]
    date = message["MessageAttributes"]["Date"]["StringValue"]
    time = message["MessageAttributes"]["Time"]["StringValue"]
    numOfPeople = message["MessageAttributes"]["NumPeople"]["StringValue"]
    phoneNumber = message["MessageAttributes"]["PhoneNum"]["StringValue"]
    email = message["MessageAttributes"]["Email"]["StringValue"]
    phoneNumber = "+1" + phoneNumber
    if not cuisine or not phoneNumber:
        return
    
    ids = []
    results = query(cuisine)
    for restaurant in results:
        ids.append(restaurant["id"])
  
    print('XYZ - results of ids:', ids)
    
    
    messageToSend = 'Hello! Here are my {cuisine} restaurant suggestions in {location} for {numPeople} people, for {diningDate} at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningDate=date,
            diningTime=time,
        )

    dynamodb_resource = resource('dynamodb', region_name='us-east-1', 
        aws_access_key_id='REDACTED',
        aws_secret_access_key= 'REDACTED' )
    
    table = dynamodb_resource.Table('yelp-restaurants')
    
    itr = 1
    for id in ids:
        itr += 1
        response = table.get_item(Key={'id': str(id)})
        print('XYZ - response:', response)
        item = response['Item']
        if response is None:
            continue
        restaurantMsg = '' + str(itr) + '. '
        name = item["name"]
        address = item["address1"]
        restaurantMsg += name +', located at ' + address +'. '
        messageToSend += restaurantMsg
        
    messageToSend += "\n\n Enjoy your meal!!"
    
    
    send_email(messageToSend, email)
    
    return {
        'statusCode': 200,
        'body': json.dumps("LF2 running succesfully")
    }
    
def send_email(messageToSend, receiver_email_address):
    client = boto3.client('ses', region_name="us-east-1")
    sender_email_address = 'mn3076@columbia.edu' # we will use the same email address for receiver and senderr
    subject_text = 'Here are your restaurant recommendations!'
    charset = "UTF-8"
    response = client.send_email(
        Destination={
            'ToAddresses': [
                receiver_email_address,
            ],
        },
        Message={
            'Body': {
                'Text': {
                    'Charset': charset,
                    'Data': messageToSend,
                }
            },
            'Subject': {
                'Charset': charset,
                'Data': subject_text,
            },
        },
        Source=sender_email_address,
    )
    print('XYZ - send_email - response', response)
    
    
def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }], http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)
        
    res = client.search(index=INDEX, body=q)
    
    print(res)
    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])
    return results



def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
        cred.secret_key,
        region,
        service,
        session_token=cred.token)
