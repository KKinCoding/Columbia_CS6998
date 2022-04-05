import json
import boto3
import logging
import inflect
from opensearchpy import OpenSearch, RequestsHttpConnection

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def search_photos(keywords):
    """
    Search photos using OpenSearch with keywords
    """
    results = []
    auth = ('master', '6998Cloud!')
    host = 'search-photos-ay6fh2jbnsmcv2md3hdxqt4z24.us-east-1.es.amazonaws.com'

    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    query = {
        'size': 1000,
        'query': {
            'match': {
                'labels': " ".join(keywords)
            }
        }
    }

    response = client.search(
        body=query,
        index='photos'
    )
    logger.debug(response)

    # get url and labels for each photo from OpenSearch response
    photo_list = response['hits']['hits']
    for photo in photo_list:
        objectKey = photo['_source']['objectKey']
        labels = photo['_source']['labels']
        photo_url = "https://cc-photos-bucket.s3.amazonaws.com/" + objectKey
        photo_info = {'url': photo_url, 'labels': labels}
        results.append(photo_info)

    return results


def get_valid_keywords(response):
    """
    Get valid keywords from Lex response, and convert words from plural to singular
    """
    keywords = []
    p = inflect.engine()
    for _, word in response['slots'].items():
        if word:
            # convert words from plural to singular if applicable
            word = word.lower()
            singular_word = p.singular_noun(word)
            if singular_word:
                keywords.append(singular_word)
            else:
                keywords.append(word)

    return keywords


def lambda_handler(event, context):
    """
    main handler of events
    """
    # get the search query q from user
    query = event['queryStringParameters']['q']

    # get keywords from the query using Lex
    client = boto3.client('lex-runtime')
    response = client.post_text(botName='SearchPhotos',
                                botAlias='SearchPhotosBot',
                                userId='testuser',
                                inputText=query)
    logger.info("Lex response: ", response)

    # get valid keywords from Lex response
    keywords = get_valid_keywords(response)
    logger.info(keywords)

    # search photos using OpenSearch with keywords
    results = []
    results += search_photos(keywords)
    logger.info(results)

    return {
        'statusCode': 200,
        'body': json.dumps({'results': results}),
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS, GET, PUT, POST',
        }
    }
