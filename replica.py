import paho.mqtt.client as mqtt
import json

MQTT_SERVER_ADDR = "localhost"
MQTT_SERVER_PORT = 1883


class Replica:
    def __init__(self, role='r', id=1, current_seq=0) -> None:
        self.role = role
        self.current_seq = current_seq
        self.id = id

        # MQTT client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(MQTT_SERVER_ADDR, MQTT_SERVER_PORT, 60)
        self.mqtt_client.loop_forever()

        self.current_seq = current_seq
        self.prepare_cache = []
        self.precommit_cache = []
        self.commit_cache = []

    def on_connect(self, client, userdata, flag, rc):
        print("Connected to MQTT server.")
        self.mqtt_client.subscribe('bft/terminal')
        self.mqtt_client.publish('bft/terminal', "I'm here")

        if self.role == 'p':
            self.mqtt_client.subscribe('bft/client_req')
            self.mqtt_client.message_callback_add('bft/client_req', self.on_client_message)

        
        self.mqtt_client.subscribe('bft/prepare')
        self.mqtt_client.message_callback_add('bft/prepare', self.on_prepare_message)
        self.mqtt_client.subscribe('bft/precommit')
        self.mqtt_client.message_callback_add('bft/precommit', self.on_precommit_message)
        self.mqtt_client.subscribe('bft/commit')
        self.mqtt_client.message_callback_add('bft/commit', self.on_commit_message)

    def on_message(self, client, usr_data, msg):
        print(msg.payload)

    def on_client_message(self, client, usr_data, msg):
        self.mqtt_client.publish('bft/prepare', self.construct_msg("Proposal from primary"))

    def on_prepare_message(self, client, usr_data, msg):
        self.prepare_cache.append(msg.payload)
        
        if len(self.prepare_cache) > 3:
            self.mqtt_client.publish('bft/precommit', self.construct_msg("vote for precommit"))

    def on_precommit_message(self, client, usr_data, msg):
        self.precommit_cache.append(msg)

        if len(self.precommit_cache) > 3:
            self.mqtt_client.publish('bft/commit', self.construct_msg("vote for commit"))

    def on_commit_message(self, client, usr_data, msg):
        self.commit_cache.append(msg)

        if len(self.commit_cache) > 3:
            self.execute()

    def validate_msg(self, msg) -> bool:
        try:
            msg = eval(msg)
            if msg['sequence_num'] == self.current_seq:
                return True
        except:
            return False # Message corrupted

    def execute(self):
        pass

    def construct_msg(self, content):
        msg = {}
        msg['replica_id'] = self.id
        msg['sequence_num'] = self.current_seq
        msg['content'] = content
        return json.dumps(msg)


if __name__ == '__main__':
    primary = Replica('p', 1)
    clients = []
    for i in range(2, 5):
        clients.append(Replica('r', i))
