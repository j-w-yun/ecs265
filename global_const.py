import paho.mqtt.client as mqtt
current_phase = "REQUEST"
mqttStuff = mqtt.Client()
topicPrefix = "ecs265pbft"