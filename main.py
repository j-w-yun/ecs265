import paho.mqtt.client as mqtt
import primary
import replica
import global_const


node_type = "P"

## Reference: https://www.eclipse.org/paho/index.php?page=clients/python/index.php


# The callback for when the client receives a CONNACK response from the server.
def on_connect(mqttStuff, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    mqttStuff.subscribe("ecs265pbft/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    if node=="P":
        primary.get_message(msg.topic, msg.payload.decode("utf-8"))
    if node.startswith("R"):
        replica.get_message(msg.topic, msg.payload.decode("utf-8"))

global_const.mqttStuff.on_connect = on_connect
global_const.mqttStuff.on_message = on_message
global_const.mqttStuff.connect("mqtt.eclipseprojects.io", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
print("Please enter the type of node\nYour options are:\nP: Primary\nR1: Replica 1\nR2: Replica 2\nR3: Replica 3")
node = str(input())
if node.startswith("R"):
    replica.init_replica(int(node[-1:]))

global_const.mqttStuff.loop_forever()