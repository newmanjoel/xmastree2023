import logging

# Add the root directory to the Python path
import os, sys

current_directory = os.path.dirname(os.path.abspath(__file__))
webservers_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(webservers_directory)

from common.common_objects import (
    setup_common_logger,
)

logger = logging.getLogger("mqtt")
logger = setup_common_logger(logger)

try:
    import paho.mqtt.client as paho
except ImportError as e:
    logger.error("Could not import the mqtt library, please run `pip install paho-mqtt`")
    sys.exit(1)


def on_message(mosq, obj, msg):
    print(f"{msg.topic} {msg.qos} {msg.payload}")
    mosq.publish('pong', 'ack', 0)

def on_publish(mosq, obj, mid):
    pass

if __name__ == '__main__':
    client = paho.Client()
    client.on_message = on_message
    client.on_publish = on_publish
    client.connect("127.0.0.1", 1883, 60)

    client.subscribe("zigbee2mqtt/motion_sensor_1/#", 0)
    client.subscribe("commands", 0)

    while client.loop() == 0:
        pass