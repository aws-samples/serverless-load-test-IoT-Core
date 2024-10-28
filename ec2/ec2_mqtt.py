import asyncio
import time
import ssl
import copy
import json
import sys
from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
#from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from queue import Queue
import threading
import socket
from datetime import datetime
import boto3

config = {} # This will be populated by reading DynamoDB
# DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='', aws_secret_access_key='')
table_name = 'loadtest-config'
table = dynamodb.Table(table_name)
response = table.get_item(Key={'yourPartitionKey': '20241021'})
config = response['Item']
#print("dynamodb config = ", config)
print("config['const_clientNumber']= ", config['const_clientNumber'])

# AWS IoT Core endpoint
ENDPOINT = config['ENDPOINT']

# Path to device certificate and key files
DEVICE_CERT = "./certs/certificate.pem.crt"
DEVICE_KEY = "./certs/private.pem.key"
ROOT_CA = "./certs/AmazonRootCA1.pem"

# MQTT config
mqtt_topic_prefix = config['mqtt_topic_prefix']
http_topic_prefix = config['http_topic_prefix']
# MQTT constant
const_clientNumber = int(config['const_clientNumber'])               # total MQTT client number, e.g. 10000, 5000, 10
const_publishActivities = int(config['const_publishActivities'])          # total publish activity number, one activity per interval: e.g. 600
const_publishInterval = float(config['const_publishInterval'])               # seconds, time of each interval: e.g. 15, 5
const_messagesPerInterval = int(config['const_messagesPerInterval'])         # publish request number per publish activity: e.g 1, 5, 
const_sleepDuration = float(config['const_sleepDuration'])           # seconds, sleeping time after each MQTT client initialization (connect & subscribe): e.g 1, 5, 10, 20, 30
# MQTT variables
message_template = {"type": "payload", "triggerNo": "-1", "threadNo": -1, "messageNo": -1, "ts": "-", "protocol": "-", "ruleNo":"-"}
CLIENTS = []
triggerNo = -1
dict_clientPubSubTs = {}    
hostname = socket.gethostname()
''' 结构如下
{
"ClientID1": [[message0_pubTs, message0_subTs], [message1_pubTs, message1_subTs], [message2_pubTs, message2_subTs], [message3_pubTs, message3_subTs]],
"ClientID2": [[message0_pubTs, message0_subTs], [message1_pubTs, message1_subTs], [message2_pubTs, message2_subTs], [message3_pubTs, message3_subTs]],
"ClientID3": [[message0_pubTs, message0_subTs], [message1_pubTs, message1_subTs], [message2_pubTs, message2_subTs], [message3_pubTs, message3_subTs]],
....   
}'''
# InfluxDB config:
influxDB_enble = config['influxDB_enble']
influx_org = config['influx_org']
influx_url = config['influx_url']
influx_token = config['influx_token']
influx_client = influxdb_client.InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
influxBucket = config['influxBucket']
const_datapointPeriod = float(config['const_datapointPeriod'])  # seconds, cycle interval to flush data points to InfluxDB, e.g. 15, 5, 3


# global variable for data points
all_connectPointsList = []
shared_queue_points = Queue()       # only for connect data points
shared_queue_pubsub = Queue()       # for data points of receiving messages (pub, sub, receive)
periodic_task_enabled = True
while_enable = True

# KDS config
kds_enable = config['kds_enable']
global_AK = config['global_AK']
global_SK = config['global_SK']
global_region = config['global_region']
global_KDS_name = config['global_KDS_name']
max_listSize = int(config['max_listSize'])  # max data points list size for KDS putrecord(), e.g. 1000, 800, 100, 3
# Create a Kinesis client
kinesis_client = boto3.client(
    'kinesis',
    aws_access_key_id=global_AK,
    aws_secret_access_key=global_SK,
    region_name=global_region
)
stream_name = global_KDS_name


# Write to KDS
def kds_write(points):     # 将data points数据写入KDS
    try:
        dataPoints = points[:]

        splitLists = [dataPoints[i:i+max_listSize] for i in range(0, len(dataPoints), max_listSize)]
        for splitlist in splitLists:
            # data to put into KDS stream
            data = {
                    'DataPoints': splitlist
                    }
            # Convert data to JSON format
            data_as_json = json.dumps(data)

            # Put data into stream
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=data_as_json,
                PartitionKey=hostname
            )
            #print("Response from KDS: ", response)

    except Exception as e:
        print(e)



'''
influx data points的格式如下:
points = [point1, point2, ...]
point = {"measurement_name":"CONNECT", "tags": tags, "fields": fields, "time": time.time_ns()}
tags = [["Protocol", "mqtt"], ["CLIENT", client_id],["RequestType", "connect"]]
fields = [["Request", 1],["Latency", 123213422]]
'''
# Write to InfluxDB, sync
def influx_write_sync(points):
    try:
        mybucket=influxBucket
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        # Write the data points to InfluxDB
        recordList = []     # pass
        for p in points:
            singleP = Point(p["measurement_name"])
            singleP.time(p["time"], write_precision=WritePrecision.NS)
            for tag in p["tags"]:
                singleP.tag(tag[0], tag[1])
            for field in p["fields"]:
                singleP.field(field[0], field[1])
            recordList.append(singleP)
        result = write_api.write(bucket=mybucket, org=influx_org, record=recordList)
        print("written influx data points: number=", len(recordList))
        write_api.close()
    except Exception as e:
        print(e)
    
    
# Write dict points to InfluxDB
# dictionary example: 
'''
[{"measurement_name": "CONNECT", "time": 121245, "tags": [["key1","value1"], ["key2","value2"]], "fields": [["key1",1], ["key2",2]]}, 
{}, {}]
'''
'''async def influx_write_async(dictPoints):
    mybucket="testBucket"
    async with InfluxDBClientAsync(url = url, token = token, org = org) as client:
        write_api = client.write_api()
        records = []
        dictPoints = [{"measurement_name": "CONNECT", "time": 121245, "tags": [["key1","value1"], ["key2","value2"]], "fields": [["key1",1], ["key2",2]]}]
        for dictP in dictPoints:
            _point = Point(dictP["measurement_name"])
            for tag in dictP["tags"]:
                _point.tag(tag[0], tag[1])
            for field in dictP["fields"]:
                _point.field(field[0], field[1])
            records.append(_point)
        successfully = await write_api.write(bucket = mybucket, record = records)
        print(f" > successfully: {successfully}")'''


## Modified from "iot device sdk python v2", samples/pubsub.py:
# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))

# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)

def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    global dict_clientPubSubTs, shared_queue_pubsub
    currentTs = time.time_ns()
    #print("Received message from topic '{}': {}, at {}".format(topic, payload, time.time()))
    message = json.loads(payload)
    try:
        dict_clientPubSubTs[message["clientID"]][message["messageNo"]-1][1] = currentTs
        # publish data points for InfluxDB
        tags = [["Protocol", "mqtt"], ["CLIENT", message["clientID"]],["RequestType", "publishOut"]]
        '''fields = [["Success", 1],["Latency", currentTs - dict_clientPubSubTs[message["clientID"]][message["messageNo"]-1][0]],["messageNo", message["messageNo"]],["Payload", str(message)]]'''
        fields = [["Success", 1],["Latency", currentTs - dict_clientPubSubTs[message["clientID"]][message["messageNo"]-1][0]],["messageNo", message["messageNo"]]]  # remove payload to save influxdb volume.
        dataPoint_pubsub = {"measurement_name":"RECEIVE", "tags": tags, "fields": fields, "time": currentTs}
        #print("receive data point: ", dataPoint_pubsub)
        shared_queue_pubsub.put(dataPoint_pubsub)
    except Exception as e:
        print(e)
        print("message payload: ", message, " PubSubTs[]= ", dict_clientPubSubTs)


# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    global dict_clientPubSubTs, shared_queue_points, all_connectPointsList
    currentTs = time.time_ns()
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    #print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))
    clientNo = str(connection.client_id).split("_")[-1]
    
    # connect data point for influxDB
    tags = [["Protocol", "mqtt"], ["CLIENT", str(connection.client_id)],["RequestType", "connect"]]
    lastconnectPoint = all_connectPointsList[int(clientNo)]
    fields = [["Success", 1],["Latency", currentTs - lastconnectPoint["time"]]]
    dataPoint_connect = {"measurement_name":"CONNECT", "tags": tags, "fields": fields, "time": currentTs}
    '''pList = []
    pList.append(dataPoint_connect)'''
    shared_queue_points.put(dataPoint_connect)
    #print("connect data point: ", dataPoint_connect)
    
    # create pub/sub timestamp list in the dictionary: dict_clientPubSubTs
    '''dict_clientPubSubTs[connection.client_id] = []      # toDelete: 考虑到reconnect情形, 这部分应该挪到connect request的地方.'''

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    #print("Connection closed")
    a = 0
##

# Asynchronous function to publish messages
async def publish_messages(client, client_no):
    global dict_clientPubSubTs, message_template
    try:
        start_time = time.monotonic()
        m = 0       # meesage number
        for i in range(const_publishActivities):  # Adjust the total publish activity number per MQTT client as required. E.g. 6, 60, 120, 1800...
            message = copy.copy(message_template)            
            message["protocol"] = "mqtt"
            message["threadNo"] = client_no
            message["clientID"] = client.client_id
            pubTopic = mqtt_topic_prefix + str(client_no)
            for k in range(const_messagesPerInterval):
                m = m +1
                message["messageNo"] = m
                message["ts"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                client.publish(topic=pubTopic, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
                dict_clientPubSubTs[client.client_id].append([time.time_ns(), -1])  # record pub tiemstamp


            elapsed_time = time.monotonic() - start_time
            '''await asyncio.sleep(const_publishInterval - elapsed_time % const_publishInterval)   # Adjust the interval time.'''
            await asyncio.sleep(const_publishInterval) # This method results in more distributed & random publishing.
    except Exception as e:
        print(e)

# Asynchronous function to start and manage MQTT clients
#async def start_clients(triggerNo):
async def start_clients():
    global dict_clientPubSubTs, triggerNo, CLIENTS, shared_queue_points, all_connectPointsList, periodic_task_enabled
    try:
        tasks = []
        start_time = time.monotonic()
        for c in range(const_clientNumber):    # Adjust the client number as required, one client per task/thread/process/request. e.g. 5, 50, 100, 200, 1000
            # MQTT Client initialization
            client_id = "C_" + str(triggerNo) + "_" + hostname + "_" + str(c)
            mqtt_connection = mqtt_connection_builder.mtls_from_path(
                endpoint=ENDPOINT,
                cert_filepath=DEVICE_CERT,
                pri_key_filepath=DEVICE_KEY,
                ca_filepath=ROOT_CA,
                on_connection_interrupted=on_connection_interrupted,
                on_connection_resumed=on_connection_resumed,
                client_id=client_id,
                clean_session=False,
                keep_alive_secs=30,
                on_connection_success=on_connection_success,
                on_connection_failure=on_connection_failure,
                on_connection_closed=on_connection_closed
            )
            
            # connect data, influxDB writing
            currentTs = time.time_ns()
            tags = [["Protocol", "mqtt"], ["CLIENT", client_id],["RequestType", "connect"]]
            fields = [["Request", 1]]
            dataPoint_connect = {"measurement_name":"CONNECT", "tags": tags, "fields": fields, "time": currentTs}
            #all_connectPoints[str(client_id)] = dataPoint_connect  # dictionary doesn't work.
            all_connectPointsList.append(dataPoint_connect)
            shared_queue_points.put(dataPoint_connect)
            
            connect_future = mqtt_connection.connect()
            connect_future.result()
            print(" Connected! ClientID = ", client_id)
            dict_clientPubSubTs[client_id] = []
        
            
            # gather all MQTT clients. Disconnect and close them after all activities.
            CLIENTS.append(mqtt_connection)
            
            # Subscribe
            subTopic = mqtt_topic_prefix + str(c)       # ToDo: unify pub topic and sub topic
            print("Subscribing to topic '{}'...".format(subTopic))
            subscribe_future, packet_id = mqtt_connection.subscribe(
                topic=subTopic,
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=on_message_received)
            subscribe_result = subscribe_future.result()
            print("Subscribed with {}".format(str(subscribe_result['qos'])), datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            
            # start publish requests.
            '''task = asyncio.create_task(publish_messages(mqtt_connection, c))
            tasks.append(task)'''
            
            # sleep after each client initialization.
            #elapsed_time = time.monotonic() - start_time
            #await asyncio.sleep(60 - elapsed_time % 60)   # Adjust the interval time, create an MQTT client per 60 second.
            await asyncio.sleep(const_sleepDuration)
            #print("toDelte...sleep over:", datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))

        # start publish requests.
        num = 0
        for singleConn in CLIENTS:
            task = asyncio.create_task(publish_messages(singleConn, num))
            tasks.append(task)
            num = num + 1

        await asyncio.gather(*tasks)
    
        for client in CLIENTS:
            #print("Disconnecting: " + client.client_id)
            client.disconnect()
        print("all mqtt clients are disconnected, number = ", len(CLIENTS))
        periodic_task_enabled = False
    except Exception as e:
        print(e)

def flush_datapoints():
    global shared_queue_points, shared_queue_pubsub
    # Convert the queue to a list and empty the queue
    list_items_connectPoints = []
    list_items_pubsubPoints = []
    with shared_queue_points.mutex: #锁住queue, 防止被修改
        list_items_connectPoints = list(shared_queue_points.queue)
        shared_queue_points.queue.clear()
    with shared_queue_pubsub.mutex:
        list_items_pubsubPoints = list(shared_queue_pubsub.queue)
        shared_queue_pubsub.queue.clear()
    if len(list_items_connectPoints) >= 1:
        if influxDB_enble : influx_write_sync(list_items_connectPoints)
        if kds_enable : kds_write(list_items_connectPoints)
        print("connect points number = ", len(list_items_connectPoints))
    if len(list_items_pubsubPoints) >= 1:
        if influxDB_enble : influx_write_sync(list_items_pubsubPoints)
        if kds_enable : kds_write(list_items_pubsubPoints)
        print("pubsub points number = ", len(list_items_pubsubPoints))

async def periodic_task():      #定时任务, 收集data point写入influxDB
    global while_enable, shared_queue_points, shared_queue_pubsub, periodic_task_enabled
    #print("toDelete, periodic_task(), while_enable, shared_queue_points, shared_queue_pubsub, periodic_task_enabled = ", while_enable, shared_queue_points, shared_queue_pubsub, periodic_task_enabled)
    while (while_enable):       
        flush_datapoints()
            
        # Sleep for a specified duration
        #await asyncio.sleep(3)  # Sleep for 3 seconds
        await asyncio.sleep(const_datapointPeriod)  # Sleep for some seconds
        if not periodic_task_enabled:
            while_enable = False
            flush_datapoints()
            print("periodic_task will not be executed next time...")

'''def run_loop_points():      #暂不使用, 用于threading
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(periodic_task())
    loop.close()'''
    
async def coroutine_main(): # 使用asyncio运行多个协程
    result1, result2 = await asyncio.gather(periodic_task(), start_clients())
    print("Result 1: ", result1)
    print("Result 2: ", result2)


# Lambda function handler
def lambda_handler(event, context):
    global triggerNo, mqtt_topic_prefix, message_template
    try:
        triggerNo = event['triggerNo']    
        mqtt_topic_prefix = mqtt_topic_prefix + str(triggerNo) + "/"
        message_template["triggerNo"] = triggerNo
        
        '''# 先观察asyncio的效果, 暂时不用Threading
        thread_1 = threading.Thread(target=run_loop_points)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_clients(triggerNo))
        loop.close()
        thread_1.start()
        thread_1.join()'''
        
        # 使用asyncio
        asyncio.run(coroutine_main())
        
        return {
            'statusCode': 200,
            'body': 'AWS IoT Core clients executed successfully'
        }
    except Exception as e:
        print(e)


# normal main entry
def main():
    global triggerNo, mqtt_topic_prefix, message_template
    try:
        triggerNo = 115     # event['triggerNo']     # for nomral python entry.
        mqtt_topic_prefix = mqtt_topic_prefix + str(triggerNo) + "/" + hostname + "/"
        message_template["triggerNo"] = triggerNo
        message_template["ruleNo"] = hostname
        
        '''# 先观察asyncio的效果, 暂时不用Threading
        thread_1 = threading.Thread(target=run_loop_points)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_clients(triggerNo))
        loop.close()
        thread_1.start()
        thread_1.join()'''
        
        # 使用asyncio
        asyncio.run(coroutine_main())
        
        print("main() done.")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()