import copy
import json

import boto3


def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = event['intermediate_s3_name']
    s3_path = '{}/additionalInfo.txt'.format(event['intermediate_directory_path'])
    additional_info = copy.deepcopy(event)
    additional_info.pop('intermediate_s3_name', None)
    additional_info.pop('intermediate_directory_path', None)

    additional_info_str = json.dumps(additional_info)
    print("Additional info: %s" % additional_info_str)

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        current_content = response['Body'].read().decode('utf-8')
        print("Reading content from file at s3:%s key:%s" % (bucket_name, s3_path))
        new_string = '{}, {}'.format(current_content, additional_info_str)
    except s3_client.exceptions.NoSuchKey:
        print("Created a new file at s3:%s key:%s" % (bucket_name, s3_path))
        new_string = additional_info_str

    encoded_string = new_string.encode("utf-8")

    response_from_s3_put = s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=encoded_string)

    return {
        'statusCode': 200,
        'body': json.dumps(response_from_s3_put)
    }
