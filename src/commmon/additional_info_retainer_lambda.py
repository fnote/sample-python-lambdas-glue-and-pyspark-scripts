import json
import boto3
import copy

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = event['intermediate_s3_name']
    s3_path = '{}/additionInfo.txt'.format(event['intermediate_directory_path'])
    additional_info = copy.deepcopy(event)
    additional_info.pop('intermediate_s3_name', None)
    additional_info.pop('intermediate_directory_path', None)

    additional_info_str = json.dumps(additional_info)
    print("Additional info: %s" % additional_info_str)

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
        currentContent = response['Body'].read().decode('utf-8')
        print("Reading content from file at s3:%s key:%s" % (bucket_name, s3_path))
        newString = '{}, {}'.format(currentContent, additional_info_str)
    except s3_client.exceptions.NoSuchKey:
        print("Created a new file at s3:%s key:%s" % (bucket_name, s3_path))
        newString = additional_info_str


    encoded_string = newString.encode("utf-8")

    s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=encoded_string)
    # TODO complete
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
