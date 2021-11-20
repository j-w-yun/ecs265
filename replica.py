import sys
import json
import string

import paho.mqtt.client as mqtt

from global_const import *


class Replica:
    def __init__(self, replica_id=0, current_seq=0, current_view=0) -> None:        
        self.id = replica_id
        self.current_seq = current_seq
        self.current_view = current_view

        self.update_role()

        self.current_phase = ConsensusPhase.IDLE
        self.log = []
        self.prepare_bit_mask = [0] * NODE_TOTAL_NUMBER
        self.commit_bit_mask = [0] * NODE_TOTAL_NUMBER
        self.client_req = ''
        self.client_req_digest = ''
        self.client_req_dict ={}

        # MQTT client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(MQTT_SERVER_ADDR, MQTT_SERVER_PORT, 60)
        self.mqtt_client.loop_forever()

    def on_connect(self, client, userdata, flag, rc):
        print("Connected to MQTT server.")
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'terminal')
        self.broadcast_msg('terminal', '{} with id {} has connected to the server.'.format('Primary' if self.role == Role.PRIMARY else 'Replica', self.id))

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
        if self.role == Role.PRIMARY:
            self.client_req_dict = self.validate_client_req(msg)
            if self.client_req_dict:
                # Update local client request info
                self.client_req = json.dumps(self.client_req_dict)
                self.client_req_digest = self.get_digest(self.client_req)

                # Update sequence number and consensus phase
                self.current_seq += 1
                self.current_phase = ConsensusPhase.PRE_PREPARE

                # Construct and broadcast the pre-prepare message as well as appending it to log
                pre_prepare_msg = self.construct_msg(if_proposal=True)
                self.log.append(pre_prepare_msg)
                self.broadcast_msg('pre-prepare', pre_prepare_msg)

    def on_pre_prepare_message(self, client, usr_data, msg):
        if self.role == Role.REPLICA:
            pre_prepare_msg_dict = self.validate_proposal(msg)
            if pre_prepare_msg_dict and self.current_phase == ConsensusPhase.IDLE:
                # Append pre-prepare message to log
                self.log.append(msg)

                # Update local client request info
                self.client_req = pre_prepare_msg_dict['m']
                self.client_req_digest = pre_prepare_msg_dict['d']
                self.client_req_dict = json.loads(self.client_req)

                # Update sequence number and consensus phase
                self.current_seq = pre_prepare_msg_dict['n']
                self.current_phase = ConsensusPhase.PREPARE

                # Construct and broadcast the prepare message as well as appending it to log
                prepare_msg = self.construct_msg()
                self.log.append(prepare_msg)
                self.broadcast_msg('prepare', prepare_msg)

    def on_prepare_message(self, client, usr_data, msg):
        prepare_msg_dict = self.validate_msg(msg)
        if prepare_msg_dict and self.current_phase == ConsensusPhase.PREPARE:
            try:
                # Update prepare bit mask: if a prepare message from replica i is accepted, self.prepare_bit_mask[i] = 1 else it is remained 0
                self.prepare_bit_mask[prepare_msg_dict['i']] = 1

                # Append prepare message to log
                self.log.append(msg)
            except IndexError:
                pass

            if len(list(filter(lambda x : x > 0, self.prepare_bit_mask))) + self.role.value > 2 * FAULT_TOLERANCE:
                # Reset prepare bit mask
                self.prepare_bit_mask = [0] * NODE_TOTAL_NUMBER

                # Update consensus phase
                self.current_phase = ConsensusPhase.COMMIT

                # Construct and broadcast the prepare message as well as appending it to log
                commit_msg = self.construct_msg()
                self.log.append(commit_msg)
                self.broadcast_msg('commit', commit_msg)

    def on_commit_message(self, client, usr_data, msg):
        commit_msg_dict = self.validate_msg(msg)
        if commit_msg_dict and self.current_phase == ConsensusPhase.COMMIT:
            try:
                # Update commit bit mask: if a commit message from replica i is accepted, self.commit_bit_mask[i] = 1 else it is remained 0
                self.commit_bit_mask[commit_msg_dict['i']] = 1

                # Append commit message to log
                self.log.append(msg)
            except IndexError:
                pass

            if len(list(filter(lambda x : x > 0, self.commit_bit_mask))) > 2 * FAULT_TOLERANCE:
                result  = self.execute(self.client_req_dict['o'])
                if result:
                    # Reset prepare bit mask
                    self.commit_bit_mask = [0] * NODE_TOTAL_NUMBER

                    # Update consensus phase
                    self.current_phase = ConsensusPhase.IDLE

                    # Construct and broadcast the prepare message as well as appending it to log
                    reply_msg = self.construct_reply('Request executed succeeded' if result else 'Request executed failed')
                    self.broadcast_msg('client', reply_msg)

    def execute(self, operation) -> bool:
        return True

    def broadcast_msg(self, topic, msg):
        self.mqtt_client.publish(MQTT_TOPIC_PREFIX + topic, msg)

    def validate_client_req(self, msg) -> dict:
        try:
            msg = json.loads(msg.payload)
            if not (msg['o'] and msg['t'] and msg['c']):
                msg = {}
            return msg
        except (json.decoder.JSONDecodeError, KeyError):
            return {}

    def validate_proposal(self, msg) -> dict:
        try:
            msg = json.loads(msg.payload)
            val_mac = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in MSG_DIGEST_KEYS}), msg['i'])
            val_digest = self.get_digest(msg['m'])
            if not (msg['mac'] == val_mac and msg['d'] == val_digest and msg['v'] == self.current_view and msg['n'] > self.current_seq):
                msg = {}
            return msg
        except (json.decoder.JSONDecodeError, KeyError) as e:
            return {}

    def validate_msg(self, msg) -> dict:
        try:
            msg = json.loads(msg.payload)
            val_mac = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in MSG_DIGEST_KEYS}), msg['i'])
            if  not (msg['mac'] == val_mac and msg['d'] == self.client_req_digest and msg['v'] == self.current_view and msg['n'] == self.current_seq):
                msg = {}
            return msg
        except (json.decoder.JSONDecodeError, KeyError):
            return {}

    def construct_msg(self, if_proposal=False) -> string:
        msg = {}
        msg['v'] = self.current_view
        msg['n'] = self.current_seq
        msg['d'] = self.client_req_digest
        msg['i'] = self.id
        msg['mac'] = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in MSG_DIGEST_KEYS}), self.id) # Currently using replica id as the parameter for key in the get_mac() 
        if if_proposal:
            msg['m'] = self.client_req

        return json.dumps(msg)

    def construct_reply(self, result_str) ->string:
        msg = {}
        msg['v'] = self.current_view
        msg['t'] = self.client_req_dict['t']
        msg['c'] = self.client_req_dict['c']
        msg['i'] = self.id
        msg['r'] = result_str

        return json.dumps(msg)

    def get_digest(self, content) -> string:
        # TODO
        return content

    def get_mac(self, msg, key) -> string:
        # TODO
        return msg

    def update_role(self): # Can be used in view change to update a replica's role
        self.role = Role.PRIMARY if self.id == self.current_view % NODE_TOTAL_NUMBER else Role.REPLICA

if __name__ == '__main__':
    primary = Replica(eval(sys.argv[1]))