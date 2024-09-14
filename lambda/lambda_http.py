import json
import boto3
import time
import asyncio
import os
import datetime

# MQTT Configuration
iotmessage = {"type": "payload", "triggerNo": -1, "threadNo": -1, "messageNo": -1, "ts": "-", "protocol": "http", "ruleNo":""}
topic_prefix = 'loadtest/http'

# Invoke IoT Core Data Plane API - Publish()
async def pub_api(request, topic_pub):
    iotdataUrl = os.environ['IOT_ENDPOINT_HTTP']
    client = boto3.client('iot-data', endpoint_url=iotdataUrl, region_name=os.environ['REGION'])
    pubTopic = topic_pub[:] + "/" + str(request)
    message = iotmessage
    message['threadNo'] = request
    for n in range(1):      # total message number per request, 1, 10, 
        message['messageNo'] = n
        message['ts'] = datetime.datetime.now().isoformat(timespec='milliseconds')
        print("topic = ", pubTopic)
        response = client.publish(
            topic = pubTopic,   # MQTT publish topic
            qos = 1,
            payload = json.dumps(message)    # please add payload generation logic according to your requirement.
            )

# Main function to establish concurrent requests, and each request invokes API independently.
async def main(mytopic):
        # Create a list of requests
        requests = []
        
        for i in range(5):  # concurrent task number, example values: 5,50,500
            requests.append(asyncio.create_task(pub_api(i, mytopic))) 
            
        # Wait for all requests to finish
        await asyncio.gather(*requests)

def lambda_handler(event, context):
    global iotmessage
    iotmessage['triggerNo'] = event['triggerNo']
    iotmessage['ruleNo'] = event['ruleNo']
    iotmessage['ts'] = event['ts']
    # Add trigger and IoT rule information to MQTT topic.
    topic = topic_prefix[:] + "/" + str(event['triggerNo']) + "/" + event['ruleNo']     
    startTs = time.time()
    try:
        asyncio.run(main(topic[:]))
    except Exception as e:
        print(e)
        
    endTs = time.time()
    return {
        'statusCode': 200,
        'body': json.dumps({"startTs": startTs, "endTs": endTs})
    }
