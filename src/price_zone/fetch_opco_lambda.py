import json
import boto3


def lambda_handler(event, context):
    client = boto3.client('s3')
    response = client.list_objects_v2(
        Bucket='rp-data-populator-poc',
        Prefix='outputStep'
    )
    var = map(lambda x: x['Key'], response['Contents'])
    return list(var)
