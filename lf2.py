import boto3
import json
import requests
import random
from package.requests_aws4auth import AWS4Auth



def receiveMsgFromSqsQueue():
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/927015531901/orderfoodqueue.fifo'
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['SentTimestamp'],
        MaxNumberOfMessages=5,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=0
        )
    print("RESPONSE: ", response)
    return response

# The function return list of business id
def findRestaurantFromElasticSearch(cuisine):
    region = 'us-east-1'
    service = 'es'
    credentials = boto3.Session(region_name="us-east-1").get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    host = 'search-dining-bot-bot3ib5rapmiqme2s3p7zg6t2i.us-east-1.es.amazonaws.com'
    index = 'restaurants'   
    url = 'https://' + host + '/' + index + '/_search'
    headers = { "Content-Type": "application/json" }
    
    es_query = "https://search-dining-bot-bot3ib5rapmiqme2s3p7zg6t2i.us-east-1.es.amazonaws.com/_search?q=" + str(cuisine)
    response = requests.get(es_query, auth=awsauth)
    res = response.json()
    print("ES RESPONSE: ", response.json())
    noOfHits = res['hits']['total']
    hits = res['hits']['hits']
    #print(noOfHits)
    #print(hits[0]['_id'])
    buisinessIds = []
    for hit in hits:
        buisinessIds.append(str(hit['_source']['id']))
    #print(len(buisinessIds))
    return buisinessIds

# function returns detail of all resturantids as a list(working)
def getRestaurantFromDb(restaurantIds):
    res = []
    client = boto3.client('dynamodb')
    for id in restaurantIds:
        response = client.get_item(TableName='yelp-restaurants', Key={'id':{'S':str(id)}})
        res.append(response)
    return res

def getMsgToSend(restaurantDetails,message):
    noOfPeople = message["MessageAttributes"]['NumPeople']["StringValue"]
    # date = message['MessageAttributes']['Date']['StringValue']
    time = message["MessageAttributes"]["Time"]["StringValue"]
    cuisine = message["MessageAttributes"]["Cuisine"]["StringValue"]
    separator = ', '
    print("RESTAURANT DETAILS:" ,restaurantDetails)
    resOneName = restaurantDetails[0]['Item']['name']["S"]
    resOneAdd = restaurantDetails[0]['Item']['address']["S"]
    resTwoName = restaurantDetails[1]['Item']['name']["S"]
    resTwoAdd = restaurantDetails[1]['Item']['address']["S"]
    resThreeName = restaurantDetails[2]['Item']['name']["S"]
    resThreeAdd = restaurantDetails[2]['Item']['address']["S"]
    msg = 'Hello! Here are my {0} restaurant suggestions for {1} people at {2} : 1. {3}, located at {4}, 2. {5}, located at {6},3. {7}, located at {8}. Enjoy your meal!'.format(cuisine,noOfPeople,time,resOneName,resOneAdd,resTwoName,resTwoAdd,resThreeName,resThreeAdd)
    return msg
    
def sendEmail(msgToSend,email):

    ses_client = boto3.client("ses", region_name="us-east-1")
    CHARSET = "UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                "am11449@nyu.edu",
            ],
        },
        Message={
            "Body": {
                "Text": {
                    "Charset": CHARSET,
                    "Data": msgToSend,
                }
            },
            "Subject": {
                "Charset": CHARSET,
                "Data": "Dining Bot Recommendations",
            },
        },
        Source="atulmb99@gmail.com",
    )
    
def deleteMsg(receipt_handle):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/927015531901/orderfoodqueue.fifo'
    sqs.delete_message(QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
    )

def lambda_handler(event, context):
    # getting response from sqs queue
    sqsQueueResponse = receiveMsgFromSqsQueue()
    if "Messages" in sqsQueueResponse.keys():
        for message in sqsQueueResponse['Messages']:
            # print("MESSAGE BODY:", message["Body"])
            # logger.debug(message)
            cuisine = message["MessageAttributes"]["Cuisine"]["StringValue"]
            restaurantIds = findRestaurantFromElasticSearch(cuisine)
            # Assume that it returns a list of restaurantsIds
            # call some random function to select 3 from the list
            restaurantIds = random.sample(restaurantIds, 3)
            restaurantDetails = getRestaurantFromDb(restaurantIds)
            # now we have all required details to send the sms
            # now we will create the required message using the details
            msgToSend = getMsgToSend(restaurantDetails,message)
            # print(msgToSend)
            # dont uncomment below line until required. There is max limit on msg
            email = message["MessageAttributes"]["Email"]["StringValue"]
            sendEmail(msgToSend,email)
            #now delete message from queue
            receipt_handle = message['ReceiptHandle']
            deleteMsg(receipt_handle)