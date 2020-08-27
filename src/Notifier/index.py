import requests
import time
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    REFERENCE_PRICING = "REFERENCE_PRICING"

    url = os.environ['cp_notification_url']
    host = os.environ['cp_notification_host']
    env = os.environ['env']
    notification_event = event.get("event", "PROCESSOR")
    status = event.get("status", "ERROR")
    message = event.get("message", "NA")
    opco_id = event.get("opco_id", "NA")
    step_function_execution_id = event.get("stepFunctionExecutionId", "")
    current_time = int(time.time())
    logger.info('Sending notification env: %s, time: %s, opco: %s, status: %s, message: %s' % (
        env, current_time, opco_id, status, message))
    data = {
        "messageAttributes": {
            "application": REFERENCE_PRICING,
            "event": notification_event,
            "status": status,
            "environment": env,
            "businessUnit": opco_id,
            "triggeredTime": current_time
        },
        "message": {
            "opco": opco_id,
            "application": REFERENCE_PRICING,
            "event": notification_event,
            "status": status,
            "message": message,
            "triggeredTime": current_time,
            "stepFunctionExecutionId": step_function_execution_id
        }
    }

    headers = {'host': host, 'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    logger.info('Response: %s' % response.json())
    return response.json()
