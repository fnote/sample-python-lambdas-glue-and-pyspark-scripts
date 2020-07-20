# Set up logging
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
import boto3
client = boto3.client('glue')

# Variables for the job:
glueJobName = "cp-ref-data-populator-sharding"

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## TRIGGERED BY EVENT: ')
    logger.info(event)
    s3 = event['Records'][0]['s3']
    s3_object_key = s3['object']['key']
    s3_path = "s" + s3['bucket']['name'] + "/" + s3_object_key
    logger.info("File Path: %s" % s3_path)
    response = client.start_job_run(JobName = glueJobName,
                                    Arguments = {
                                        '--s3_path': s3_path})
    logger.info('## STARTED GLUE JOB: ' + glueJobName)
    logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response
