import json
from datetime import datetime
import decimal
import requests
import boto3
from requests_aws4auth import AWS4Auth

API_HOST = "https://api.yelp.com"
SEARCH_PATH = "/v3/businesses/search"
BUSINESS_PATH = "/v3/businesses/"
API_KEY = "H4qlt6I7CEa7kMcshJkLjABDmw9zH5h7bp2NmJc8VADPDdpIkhpkCiZqyiTmrGLURIBNspf1dM-NZ93OR1XSjVjkxgmi-xBXg3-RcG4sm21t2zXbp3Ew3ULZEIwRYnYx"

DEFAULT_TERM = "Chinese"
DEFAULT_LOCATION = "Manhattan"
SEARCH_LIMIT = 1
RESTAURANT_COUNT = 900


def formatResponse(response):
    item = response["businesses"][0]
    data = {}
    data["Business_ID"] = item["id"]
    data["Name"] = item["name"]
    data["Address"] = ", ".join(item["location"]["display_address"])
    data["Coordinates"] = {
                            "latitude": decimal.Decimal(str(item["coordinates"]["latitude"])),
                            "longitude": decimal.Decimal(str(item['coordinates']["longitude"]))
                        }
    data["Reviews"] = item["review_count"]
    data["Rating"] = decimal.Decimal(str(item["rating"]))
    data["Zip_Code"] = item["location"]["zip_code"]
    data["insertedAtTimestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return data


def insertDynamoDB(data, name="yelp-restaurants"):
    db = boto3.resource("dynamodb")
    table = db.Table(name)
    response = table.put_item(Item=data)
    return


def insertElasticSearch(id, cuisine):
    region = "us-east-1"
    service = "es"
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    url = "https://search-elasticsearch-rbcr4qxzwbte3wt4giculqncli.us-east-1.es.amazonaws.com/restaurants/Restaurant"
    data = {}
    data["Business_ID"] = id
    data["Cuisine"] = cuisine
    data = json.dumps(data)
    headers = {"content-type":"application/json"}
    response = requests.post(url=url, data=data, headers=headers, auth=awsauth)
    return


def search(cuisine_ls):
    '''
    Given a list of cuisines, search for every cuisine corresponding restaurants
    '''
    url = API_HOST + SEARCH_PATH
    headers = {
        'Authorization': 'Bearer %s' % API_KEY,
    }
    for cuisine in cuisine_ls:
        print("Searching for " + cuisine)
        number_restaurants = 0
        offset = 0
        while number_restaurants < RESTAURANT_COUNT:
            url_params = {
                'term': cuisine.replace(' ', '+'),
                'location': DEFAULT_LOCATION,
                'offset': offset,
                'limit': SEARCH_LIMIT
            }
            response = requests.request('GET', url, headers=headers, params=url_params).json()
            if "businesses" in response:
                data = formatResponse(response)
                try:
                    insertDynamoDB(data)
                    insertElasticSearch(data["Business_ID"], cuisine)
                    number_restaurants += 1
                except:
                    print("Insertion failed.")
            offset += 1
    print("Searching finish!\n")


cuisines = ["Chinese", "Indian", "Mexican", "Bread", "Ice cream", "Sushi", "Burger"]
search(cuisines)