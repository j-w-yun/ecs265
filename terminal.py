import paho.mqtt.client as mqtt
from datetime import datetime


def on_connect(client, usr_data, flag, rc):
    print('Terminal connected.')

def on_message(client, usr_data, msg):
    try:
        # msg = eval(str(msg.payload))
        print(datetime.now(), msg)
    except:
        print(datetime.now(), ':', 'Received a corrupted message.') # Message corrupted
    
    
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect('localhost', 1883, 60)
client.subscribe('bft/#')
client.loop_forever()
