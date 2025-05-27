"""
AWS IoT Core Concurrent Publisher Lambda Function

This Lambda function uses AWS IoT Device SDK for Python v2 to establish multiple
MQTT long connections and publish messages to a large number of topics concurrently.
It can be run as a Lambda function or locally as a Python script.

Features:
- Uses websocket connections with IAM authentication
- Creates multiple threads, one per MQTT client
- Each client publishes to multiple topics at 50 TPS
- Synchronizes publishing to start at the next minute's 0 second mark
- Uses QoS 1 for at-least-once delivery
- Handles topics from test event or generates test topics

Environment Variables:
- IOT_ENDPOINT: AWS IoT endpoint (default: xxxxxx.iot.ap-northeast-1.amazonaws.com)
"""

import os
import json
import time
import uuid
import random
import string
import logging
import threading
import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any, Optional

# Import AWS IoT Device SDK v2 for Python
from awscrt import io, mqtt, auth
from awsiot import mqtt_connection_builder

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
DEFAULT_ENDPOINT = "xxxxxxx-ats.iot.ap-northeast-1.amazonaws.com"
TOPICS_PER_CLIENT = 50  # Number of topics each client will handle
PUBLISH_RATE = 50  # Messages per second per client
QOS = mqtt.QoS.AT_LEAST_ONCE
MESSAGE_RETENTION = 60  # Message retention time in seconds
MAX_CLIENTS = 300  # Maximum number of MQTT clients to create

# Global variables
connected_clients = 0
connection_lock = threading.Lock()
all_connected_event = threading.Event()
start_publish_event = threading.Event()
publish_complete_event = threading.Event()
publish_results = {
    "total_published": 0,
    "successful": 0,
    "failed": 0,
    "clients": 0,
    "start_timestamp": None,
    "end_timestamp": None
}
publish_results_lock = threading.Lock()

def on_connection_interrupted(connection, error, **kwargs):
    """Callback when connection is accidentally lost."""
    logger.warning(f"Connection interrupted. error: {error}")

def on_connection_resumed(connection, return_code, session_present, **kwargs):
    """Callback when an interrupted connection is re-established."""
    logger.info(f"Connection resumed. return_code: {return_code}, session_present: {session_present}")

def on_connection_success(connection, callback_data):
    """Callback when connection is established successfully."""
    global connected_clients
    client_id = callback_data
    
    with connection_lock:
        connected_clients += 1
        logger.info(f"Connection established for client {client_id}. Total connected: {connected_clients}/{callback_data.split('-')[-1]}")
        
        # Check if all clients are connected
        if connected_clients >= int(callback_data.split('-')[-1]):
            all_connected_event.set()

def on_connection_failure(connection, callback_data):
    """Callback when connection fails."""
    client_id = callback_data
    logger.error(f"Connection failed for client {client_id}")

def create_mqtt_connection(client_id: str, endpoint: str) -> mqtt.Connection:
    """Create an MQTT connection using websockets with IAM authentication."""
    # Create a connection using websockets with IAM credentials
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
    # Create credentials provider
    credentials_provider = auth.AwsCredentialsProvider.new_default_chain(client_bootstrap)
    
    connection = mqtt_connection_builder.websockets_with_default_aws_signing(
        endpoint=endpoint,
        credentials_provider=credentials_provider,
        client_bootstrap=client_bootstrap,
        region=os.environ.get("AWS_REGION", "ap-northeast-1"),
        client_id=client_id,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        clean_session=True,
        keep_alive_secs=30
    )
    
    # Connect and wait for connection to be established
    connect_future = connection.connect()
    connect_future.result()  # Wait for connection to complete
    
    # Manually call our success callback
    on_connection_success(connection, client_id)
    
    return connection

def publish_messages(connection: mqtt.Connection, topics: List[str], client_id: str):
    """Publish messages to the assigned topics at the specified rate."""
    logger.info(f"Client {client_id} ready to publish to {len(topics)} topics")
    
    # Wait for the signal to start publishing
    start_publish_event.wait()
    
    # Calculate delay between each batch to achieve the desired publish rate
    delay = len(topics) * 1.0 / PUBLISH_RATE if PUBLISH_RATE > 0 else 0
    
    try:
        for topic in topics:
            # Create message payload
            message = {
                "client_id": client_id,
                "topic": topic,
                "timestamp": int(time.time()),
                "message_id": str(uuid.uuid4()),
                "data": f"Test message for {topic}"
            }
            
            try:
                # Publish message with QoS 1
                publish_result = connection.publish(
                    topic=topic,
                    payload=json.dumps(message),
                    qos=QOS,
                    retain=False
                )
                
                # Handle the result - could be a tuple or a Future
                if hasattr(publish_result, 'result'):
                    # It's a Future object
                    publish_result.result()  # Wait for completion
                    with publish_results_lock:
                        publish_results["successful"] += 1
                        publish_results["total_published"] += 1
                else:
                    # It's likely a tuple with (packet_id, QoS)
                    with publish_results_lock:
                        publish_results["successful"] += 1
                        publish_results["total_published"] += 1
                        
            except Exception as e:
                with publish_results_lock:
                    publish_results["failed"] += 1
                    publish_results["total_published"] += 1
                logger.error(f"Failed to publish to topic '{topic}' from client {client_id}: {e}")
            
        # Sleep to maintain the publish rate
        time.sleep(delay)
    
    except Exception as e:
        logger.error(f"Error in publish_messages for client {client_id}: {e}")
    
    logger.info(f"Client {client_id} completed publishing")

def client_thread_function(client_id: str, topics: List[str], endpoint: str):
    """Function that runs in each client thread."""
    try:
        # Create MQTT connection
        connection = create_mqtt_connection(client_id, endpoint)
        
        # Wait for all clients to connect
        all_connected_event.wait()
        
        # Publish messages
        publish_messages(connection, topics, client_id)
        
        # Disconnect after publishing
        connection.disconnect()
        
    except Exception as e:
        logger.error(f"Error in client_thread_function for client {client_id}: {e}")

def wait_until_next_minute():
    """Wait until the start of the next minute."""
    now = datetime.datetime.now()
    next_minute = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=1)
    wait_seconds = (next_minute - now).total_seconds()
    
    logger.info(f"Waiting {wait_seconds:.2f} seconds until {next_minute.strftime('%H:%M:%S')} to start publishing")
    time.sleep(wait_seconds)

def lambda_handler(event, context):
    """Lambda function handler."""
    global connected_clients, publish_results
    
    # Reset global state
    connected_clients = 0
    all_connected_event.clear()
    start_publish_event.clear()
    publish_complete_event.clear()
    publish_results = {
        "total_published": 0,
        "successful": 0,
        "failed": 0,
        "clients": 0,
        "start_timestamp": None,
        "end_timestamp": None
    }
    
    start_time = time.time()
    
    try:
        # Get IoT endpoint from environment variable or use default
        endpoint = os.environ.get("IOT_ENDPOINT", DEFAULT_ENDPOINT)
        
        # Get topics from the event or generate test topics
        topics = event.get("topics", [])
        if not topics:
            # Generate 10,000 test topics if none provided
            topics = [f"concurrentPub/topic/{i+1}" for i in range(10000)]
        
        total_topics = len(topics)
        logger.info(f"Processing {total_topics} topics")
        
        # Calculate number of clients needed
        num_clients = min(MAX_CLIENTS, (total_topics + TOPICS_PER_CLIENT - 1) // TOPICS_PER_CLIENT)
        publish_results["clients"] = num_clients
        
        logger.info(f"Creating {num_clients} MQTT clients")
        
        # Distribute topics among clients
        client_topics = []
        for i in range(num_clients):
            start_idx = i * TOPICS_PER_CLIENT
            end_idx = min(start_idx + TOPICS_PER_CLIENT, total_topics)
            client_topics.append(topics[start_idx:end_idx])
        
        # Create and start client threads
        threads = []
        for i in range(num_clients):
            # generate random client ID with 5 random letters, as this Lambda function will be invoked concurrently.
            random_letters = ''.join(random.choice(string.ascii_lowercase) for _ in range(5))
            client_id = f"mqtt-{random_letters}-{i+1}-{num_clients}"
            thread = threading.Thread(
                target=client_thread_function,
                args=(client_id, client_topics[i], endpoint)
            )
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Wait for all clients to connect
        logger.info("Waiting for all clients to connect...")
        all_connected_event.wait(timeout=30)  # Wait up to 30 seconds for connections
        
        if connected_clients < num_clients:
            logger.warning(f"Only {connected_clients}/{num_clients} clients connected within timeout")
        
        # Wait until the start of the next minute to begin publishing
        wait_until_next_minute()
        
        # Signal all threads to start publishing
        logger.info("Starting concurrent publishing")
        start_time_publish = time.time()
        publish_results["start_timestamp"] = datetime.datetime.now().isoformat()
        start_publish_event.set()
        
        # Wait for all threads to complete (with timeout for Lambda)
        remaining_time = context.get_remaining_time_in_millis() / 1000 - 5 if context else 290
        for thread in threads:
            thread.join(timeout=remaining_time / len(threads))
        
        # Record end timestamp
        publish_results["end_timestamp"] = datetime.datetime.now().isoformat()
        
        # Calculate statistics
        end_time = time.time()
        total_duration = end_time - start_time
        publish_duration = end_time - start_time_publish
        
        result = {
            "statusCode": 200,
            "body": {
                "message": "Concurrent publishing completed",
                "statistics": {
                    "total_topics": total_topics,
                    "clients_used": num_clients,
                    "total_published": publish_results["total_published"],
                    "successful": publish_results["successful"],
                    "failed": publish_results["failed"],
                    "total_duration_seconds": total_duration,
                    "publish_duration_seconds": publish_duration,
                    "messages_per_second": publish_results["successful"] / publish_duration if publish_duration > 0 else 0,
                    "start_timestamp": publish_results["start_timestamp"],
                    "end_timestamp": publish_results["end_timestamp"]
                }
            }
        }
        
        logger.info(f"Result: {json.dumps(result)}")
        return result
        
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": {
                "message": f"Error: {str(e)}",
                "statistics": publish_results
            }
        }

def run_locally():
    """Run the function locally for testing."""
    class MockContext:
        def get_remaining_time_in_millis(self):
            return 300000  # 5 minutes
    
    # Create a test event with topics
    test_event = {
        "topics": [f"concurrentPub/topic/{i+1}" for i in range(10)]  # Use fewer topics for local testing
    }
    
    # Call the lambda handler
    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    # Configure logging for local execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the function locally
    run_locally()
