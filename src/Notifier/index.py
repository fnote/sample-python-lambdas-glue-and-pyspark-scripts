import requests
import time
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    REFERENCE_PRICING = "REFERENCE_PRICING"
    current_time = int(time.time())
    event_component = event.get("event", "PROCESSOR")
    status = event.get("status", "ERROR")
    env = os.environ['env']
    message = event.get("message", "NA")
    opco_id = event.get("opco_id", "NA")

    logger.info('Sending notification env: %s, time: %s, opco: %s, status: %s, message: %s' % (
        env, current_time, opco_id, status, message))
    data = {
        "messageAttributes": {
            "application": REFERENCE_PRICING,
            "event": event_component,
            "status": status,
            "environment": env,
            "businessUnit": opco_id,
            "triggeredTime": current_time
        },
        "message": {
            "opco": opco_id,
            "application": REFERENCE_PRICING,
            "event": event_component,
            "status": status,
            "message": message,
            "triggeredTime": current_time,
        }
    }
    url = 'https://vpce-0ee878fea3ffd19c4-6bsoxvze.execute-api.us-east-1.vpce.amazonaws.com/stg/v1/cpns/notifications'
    headers = {'host': 'ivsgfq5vdl.execute-api.us-east-1.amazonaws.com', 'Content-Type': 'application/json'}
    response = requests.post(url, json=data, headers=headers)
    logger.info('Response: %s' % response.json())
    return response.json()
