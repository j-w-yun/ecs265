import paho.mqtt.client as mqtt


client = mqtt.Client()
client.connect('localhost', 1883, 60)
client.subscribe('bft/#')
client.publish('bft/client_req', 'Here is a request')
client.loop_forever()