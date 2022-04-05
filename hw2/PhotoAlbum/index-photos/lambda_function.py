import json
import boto3
import logging
import time
import os
from datetime import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def detect_labels(photo, bucket):
    """
    Use Rekognition to detect labels from the photo
    """
    client = boto3.client('rekognition')
    response = client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': photo}},
                                    MaxLabels=10)
    labels = [label['Name'].lower() for label in response['Labels']]

    return labels


def OpenSearch_store(json_array, key):
    """
    Store a JSON object in an OpenSearch index ('photos')
    """

    auth = ('master', '6998Cloud!')
    host = 'search-photos-ay6fh2jbnsmcv2md3hdxqt4z24.us-east-1.es.amazonaws.com'

    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    response = client.index(
        index='photos',
        doc_type='_doc',
        id=key,
        body=json.dumps(json_array).encode('utf-8'),
        refresh=True)

    logger.debug("OpenSearch response: ", response)

    return response


def lambda_handler(event, context):
    """
    main handler of events
    """
    # set the default time zone
    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    # Get s3 bucket and key from the event
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # get detection labels from Rekognition
    labels = detect_labels(key, bucket)

    logger.debug("Labels detected: " + ', '.join(labels))

    # get custom labels from users if applicable
    try:
        metadata = s3.head_object(Bucket=bucket, Key=key)
        logger.info(metadata)
        custom_labels = metadata['ResponseMetadata']['HTTPHeaders']['x-amz-meta-customlabels']
        logger.info(custom_labels)
        if custom_labels:
            labels += [label.strip().lower() for label in custom_labels.split(',')]
    except Exception as e:
        logger.debug("No custom labels.")

    # create a JSON array (A1) with the labels
    json_array = {
        'objectKey': key,
        'bucket': bucket,
        'createdTimestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'labels': labels
    }

    # store the JSON object in OpenSearch
    response = OpenSearch_store(json_array, key)

    return {
        'statusCode': 200,
        'body': 'Finished indexing LF1.',
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,PUT',
        }
    }
