import json
import boto3
import datetime
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
import csv
from io import BytesIO
from requests_aws4auth import AWS4Auth

host = 'search-dining-bot-bot3ib5rapmiqme2s3p7zg6t2i.us-east-1.es.amazonaws.com' 
credential = boto3.Session(aws_access_key_id="",
                          aws_secret_access_key="", 
                          region_name="us-east-1").get_credentials()
region = 'us-east-1'
service = 'es'
auth = AWS4Auth(credential.access_key, credential.secret_key, region, service)

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection   
)
print(es)
with open('C:\\Data\\NYU\\CourseWork\\Fall 2022\\Cloud Compute\\AS2\\yelpScrape.csv', newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    restaurants = list(reader)

for restaurant in restaurants:
    index_data = {
        'id': restaurant[0],
        'cuisines': restaurant[7]
    }
    # index_data["id"] = str(restaurant[0])
    # index_data["cuisine"] = restaurant[7]
    print ('dataObject', index_data)

    es.index(index="restaurants", doc_type="Restaurant", body=index_data, id = restaurant[0], refresh=True)