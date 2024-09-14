from awscrt import mqtt, http, auth
from awsiot import mqtt_connection_builder
from awscrt.io import SocketOptions
import sys
import threading
import time
import json
from datetime import timedelta
import datetime

import schedule

# user configuration
iotendpoint = "Your IoT Core Endpoint" # e.g. "xxxxxx-ats.iot.ap-northeast-1.amazonaws.com"
certpath = "Your Certificate Path" # e.g. "Certificate_Directory/cert.pem.crt"
privatepath = "Your Private Key Path"   # e.g. "Certificate_Directory/private.key"
awsRegion = "Your AWS Region" # e.g. 'ap-northeast-1' 

# mqtt configuration
ruleTopic = 'loadtest/lambda/http'
mqtt_connection = 0
count = 0

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
    print("Received message from topic '{}': {}".format(topic, payload))
    global received_count
    received_count += 1
    '''if received_count == cmdData.input_count:
        received_all_event.set()'''

# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

def publish_mqtt():
    global count
    print("Time: ", datetime.datetime.now(datetime.UTC))
    for m in range(1):      # 每次发送多少个trigger, example values = 1, 5, 10, 20.
        count +=1
        # single publish request:
        message_topic = ruleTopic
        message_json = {"type": "trigger","triggerNo": -1, "ts":""}
        message_json["triggerNo"] = count
        message_json["ts"] = datetime.datetime.now().isoformat(timespec='milliseconds')
        print("Publishing trigger to topic '{}': {}, No.{}".format(message_topic, message_json, count))
        mqtt_connection.publish(
            topic=message_topic,
            payload=json.dumps(message_json),
            qos=mqtt.QoS.AT_LEAST_ONCE)

if __name__ == '__main__':
    try:
        print("starts...")
        # MQTT Client initilization and connect
        clientid = "pubMQTT_" + str(time.time())
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=iotendpoint,
            port=8883,
            cert_filepath=certpath,
            pri_key_filepath=privatepath,
            on_connection_interrupted=on_connection_interrupted,
            on_connection_resumed=on_connection_resumed,
            client_id=clientid,
            on_connection_success=on_connection_success,
            on_connection_failure=on_connection_failure,
            on_connection_closed=on_connection_closed)
        connect_future = mqtt_connection.connect()
        # Future.result() waits until a result is available
        connect_future.result()

        # repeat publishing
        n =  121       # 每1秒发送一次mqtt publish, 持续 n 秒, n = 2, 1801, 11, 101, 201, 301
        #schedule.every(5).seconds.until(timedelta(seconds=n)).do(publish_mqtt)  # 每5秒
        #schedule.every(2).seconds.until(timedelta(seconds=n)).do(publish_mqtt)  # 每2秒
        schedule.every().second.until(timedelta(seconds=n)).do(publish_mqtt)  # 每1秒
        while True:
            schedule.run_pending()
        
        publishMQTT()
        '''for i in range(100):
            t = threading.Thread(target=publishMQTT)
            t.start()'''
        # Disconnect
        print("thread: ", threadName, ", Disconnecting...")
        disconnect_future = mqtt_connection.disconnect()
        disconnect_future.result()
        print("thread: ", threadName, ", Disconnected!")
    except Exception as err:
        print(err)