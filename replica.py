import json
import string

import paho.mqtt.client as mqtt

from global_const import *


class Replica:
    def __init__(self, role='r', replica_id=1, current_seq=0) -> None:
        self.role = Role.PRIMARY if role == 'p' else Role.REPLICA
        self.current_seq = current_seq
        self.id = replica_id

        self.current_seq = current_seq
        self.current_phase = ConsensusPhase.IDLE
        self.prepare_cache = []
        self.commit_cache = []

        # MQTT client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(MQTT_SERVER_ADDR, MQTT_SERVER_PORT, 60)
        self.mqtt_client.loop_forever()

    def on_connect(self, client, userdata, flag, rc):
        print("Connected to MQTT server.")
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'terminal')
        self.broadcast_msg('terminal', "I'm here")

        if self.role == Role.PRIMARY:
            self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'client_req')
            self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'client_req', self.on_client_message)

        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'pre-prepare')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'pre-prepare', self.on_pre_prepare_message)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'prepare')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'prepare', self.on_prepare_message)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'commit')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'commit', self.on_commit_message)

    def on_message(self, client, usr_data, msg):
        pass

    def on_client_message(self, client, usr_data, msg):
        self.current_phase = ConsensusPhase.PRE_PREPARE
        self.current_seq += 1
        self.broadcast_msg('pre-prepare', 'Proposal from primary')

    def on_pre_prepare_message(self, client, usr_data, msg):
        msg = self.validate_msg(msg)
        if msg and self.role == Role.REPLICA and msg['role'] == Role.PRIMARY.value and self.current_phase == ConsensusPhase.IDLE: 
                if msg['current_seq'] > self.current_seq:
                    self.current_seq = msg['current_seq']
                    self.current_phase = ConsensusPhase.PRE_PREPARE
                    self.broadcast_msg('prepare', 'Vote for prepare certificate')

    def on_prepare_message(self, client, usr_data, msg):
        msg = self.validate_msg(msg)
        if msg and msg['current_seq'] == self.current_seq and self.current_phase == ConsensusPhase.PRE_PREPARE:
            self.prepare_cache.append(msg)

            if len(self.prepare_cache) + self.role.value > 2 * FAULT_TOLERANCE:
                self.prepare_cache.clear()
                self.current_phase = ConsensusPhase.COMMIT
                self.broadcast_msg('commit', 'Vote for commit')

    def on_commit_message(self, client, usr_data, msg):
        msg = self.validate_msg(msg)
        if msg and msg['current_seq'] == self.current_seq and self.current_phase == ConsensusPhase.COMMIT:
            self.commit_cache.append(msg)

            if len(self.commit_cache) > 2 * FAULT_TOLERANCE:
                result  = self.execute()
                if result:
                    self.commit_cache.clear()
                    self.current_phase = ConsensusPhase.IDLE
                    self.broadcast_msg('client', 'Request executed')

    def validate_msg(self, msg) -> bool:
        try:
            msg = json.loads(msg.payload)
            return msg
        except:
            return

    def execute(self) -> bool:
        return True

    def construct_msg(self, content) -> string:
        msg = {}
        msg['id'] = self.id
        msg['role'] = self.role.value
        msg['current_seq'] = self.current_seq
        msg['current_phase'] = self.current_phase.value

        if isinstance(content, dict):
            for key, value in content.items():
                msg[key] = value
        else:
            msg['meta'] = str(content)

        return json.dumps(msg)


    def broadcast_msg(self, topic, msg):
        msg = self.construct_msg(msg)
        self.mqtt_client.publish(MQTT_TOPIC_PREFIX + topic, msg)


if __name__ == '__main__':
    primary = Replica('r', 4)
