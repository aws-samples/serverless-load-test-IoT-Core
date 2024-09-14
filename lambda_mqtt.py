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

# AWS IoT Core endpoint
ENDPOINT = 'xxxxx-ats.iot.ap-southeast-1.amazonaws.com'

# Path to device certificate and key files
DEVICE_CERT = "certs/certificate.crt.pem"
DEVICE_KEY = "certs/private.key"
ROOT_CA = "certs/rootCA.pem"
# MQTT config
mqtt_topic_prefix = "loadtest/mqtt/"
http_topic_prefix = "loadtest/http/"
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
org = "aws"
url = "https://xxxxxx.ap-northeast-1.timestream-influxdb.amazonaws.com:8086"
token = "xxxxxxx"
influx_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# global variable for data points
all_connectPointsList = []
shared_queue_points = Queue()       # only for connect data points
shared_queue_pubsub = Queue()       # for data points of receiving messages (pub, sub, receive)
periodic_task_enabled = True
while_enable = True

'''class InfluxDataPoint_Connect:
    #dictionary = {"measurement": "", "tags": {}, "fields":{}, "time": 0} # dict according to InfluxDB client Point.fromDict()
    dictionary = {"measurement": "", "tags": {}, "fields":{}}
    sessionID = ""
    clientID = ""
    connectInitTime = 0
    over = False
    def __init__(self, session_id, client_id):
        self.sessionID = session_id
        self.clientID = client_id
    def setSessionID(self, session_id):
        self.sessionID = session_id
    def setClientID(self, client_id):
        self.clientID = client_id
    def setMeasurement(self, measurement_name):
        self.dictionary["measurement"] = measurement_name
    def setTime(self, ts):
        self.dictionary["time"] = ts   # test, 先不设定时间
    def addTag(self, key, value):
        self.dictionary["tags"][key] = value
    def addField(self, key, value):
        self.dictionary["fields"][key] = value
    def getPoint(self):
        print("dict = " + str(self.dictionary))
        objectPoint = influxdb_client.client.write.point.Point(self.dictionary["measurement"]).from_dict(self.dictionary, WritePrecision.NS)
        print(objectPoint)
        return objectPoint'''
        

# Write to InfluxDB, async
async def influx_write(points):     # 暂时不用, 异步写入模式还未调试
    # points should be a list of Point: 
    print("influx_write() start, " + str(time.time()))
    mybucket="testBucket"
    write_api = influx_client.write_api(write_options=ASYNCHRONOUS)
    # Write the data points to InfluxDB
    async_Results = write_api.write(bucket=mybucket, record=points)
    print("async_Result done.")
    for async_Result in async_Results:
        await async_Result.get()
    print("influx_write() end, " + str(time.time()))
    write_api.close()


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
        mybucket="testBucket"
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
        result = write_api.write(bucket=mybucket, org="aws", record=recordList)
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
    dict_clientPubSubTs[message["clientID"]][message["messageNo"]-1][1] = currentTs
    # publish data points for InfluxDB
    tags = [["Protocol", "mqtt"], ["CLIENT", message["clientID"]],["RequestType", "publishOut"]]
    fields = [["Success", 1],["Latency", currentTs - dict_clientPubSubTs[message["clientID"]][message["messageNo"]-1][0]],["messageNo", message["messageNo"]],["Payload", str(message)]]
    dataPoint_pubsub = {"measurement_name":"RECEIVE", "tags": tags, "fields": fields, "time": currentTs}
    #print("receive data point: ", dataPoint_pubsub)
    shared_queue_pubsub.put(dataPoint_pubsub)

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
    dict_clientPubSubTs[connection.client_id] = []      # toDo: 考虑到reconnect情形, 这部分应该挪到connect request的地方.

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
        messagesPerInterval = 6            # Adjust the publishRequest number each time, max 50/s
        m = 0
        for i in range(800):  # Adjust the total message number per MQTT client as required. E.g. 6, 60, 120, 1800...
            message = copy.copy(message_template)
            #message["ts"] = time.time()
            message["ts"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            message["protocol"] = "mqtt"
            message["threadNo"] = client_no
            message["clientID"] = client.client_id
            pubTopic = mqtt_topic_prefix + str(client_no)
            
            for k in range(messagesPerInterval):
                m = m +1
                message["messageNo"] = m
                message["ts"] = time.time()
                client.publish(topic=pubTopic, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
                dict_clientPubSubTs[client.client_id].append([time.time_ns(), -1])  # record pub tiemstamp
                #print("toDelete. messageNo = ", m, ", pub timestamp = ", dict_clientPubSubTs[client.client_id])

            elapsed_time = time.monotonic() - start_time
            await asyncio.sleep(1 - elapsed_time % 1)   # Adjust the interval time, send a message per 1 second.
    except Exception as e:
        print(e)

# Asynchronous function to start and manage MQTT clients
#async def start_clients(triggerNo):
async def start_clients():
    global dict_clientPubSubTs, triggerNo, CLIENTS, shared_queue_points, all_connectPointsList, periodic_task_enabled
    try:
        tasks = []
        start_time = time.monotonic()
        for c in range(10):    # Adjust the client number as required, one client per task/thread/process/request. e.g. 5, 50, 100, 200, 1000
            # MQTT Client initialization
            client_id = "CLIENT_" + str(triggerNo) + "_" + hostname + "_" + str(c)
            mqtt_connection = mqtt_connection_builder.mtls_from_path(
                endpoint=ENDPOINT,
                cert_filepath=DEVICE_CERT,
                pri_key_filepath=DEVICE_KEY,
                ca_filepath=ROOT_CA,
                on_connection_interrupted=on_connection_interrupted,
                on_connection_resumed=on_connection_resumed,
                client_id=client_id,
                clean_session=True,
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
            print("Subscribed with {} ".format(str(subscribe_result['qos'])), subTopic)
    
            '''task = asyncio.create_task(publish_messages(mqtt_connection, c))
            tasks.append(task)'''
            
            # sleep after each client initialization.
            #elapsed_time = time.monotonic() - start_time
            #await asyncio.sleep(60 - elapsed_time % 60)   # Adjust the interval time, create an MQTT client per 60 second.
            await asyncio.sleep(30)
            
        # start publish
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
        influx_write_sync(list_items_connectPoints)
        print("connect points number = ", len(list_items_connectPoints))
    if len(list_items_pubsubPoints) >= 1:
        influx_write_sync(list_items_pubsubPoints)
        print("pubsub points number = ", len(list_items_pubsubPoints))

async def periodic_task():      #定时任务, 收集data point写入influxDB
    global while_enable, shared_queue_points, shared_queue_pubsub, periodic_task_enabled
    
    while (while_enable):       
        flush_datapoints()
            
        # Sleep for a specified duration
        await asyncio.sleep(3)  # Sleep for 3 seconds
        if not periodic_task_enabled:
            while_enable = False
            flush_datapoints()
            print("periodic_task will not be executed next time...")
    
async def coroutine_main(): # 使用asyncio运行多个协程
    result1, result2 = await asyncio.gather(periodic_task(), start_clients())
    print("Result 1: ", result1)
    print("Result 2: ", result2)


# Lambda function handler
def lambda_handler(event, context):
    global triggerNo, mqtt_topic_prefix, message_template
    try:
        triggerNo = event['triggerNo']    
        mqtt_topic_prefix = mqtt_topic_prefix + str(triggerNo) + "/" + hostname + "/"
        message_template["triggerNo"] = triggerNo
        message_template["ruleNo"] = hostname
        
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
        
        # 使用asyncio
        asyncio.run(coroutine_main())
        
        print("main() done.")
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()