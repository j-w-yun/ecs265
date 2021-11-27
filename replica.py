import hashlib
import json
import string
import sys
from threading import Thread,Event,Timer

# import paho.mqtt.client as mqtt
import socket_client as mqtt
print(mqtt)

from global_const import *


class WaitTimer:
    def __init__(self,function):
        Thread.__init__(self)
        self.interval = 60
        self.function = function
        self.finished = Event()
        self.resetted = True

    def cancel(self):
        self.finished.set()

    def run(self):
        while self.resetted:
            self.resetted = False
            self.finished.wait(self.interval)
        if not self.finished.isSet():
            self.function()
        self.finished.set()

    def reset(self):
        self.resetted = True
        self.finished.set()
        self.finished.clear()


class Replica:
    # def __init__(self, role='r', replica_id=1, current_seq=0) -> None:
    def __init__(self, replica_id=0, current_seq=0, current_view=0) -> None:
        self.id = replica_id
        self.current_seq = current_seq
        self.current_view = current_view
        self.state = State.READY
        self.timer = WaitTimer(self.init_view_change)
        self.requests = 0
        self.role = Role.REPLICA

        self.update_role()
        print('role', self.role)

        self.current_phase = ConsensusPhase.IDLE
        self.log = {}
        self.reply_history = {}

        self.client_req = ''
        self.client_req_digest = ''
        self.client_req_dict ={}

        # MQTT client setup
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(MQTT_SERVER_ADDR, MQTT_SERVER_PORT)
        self.mqtt_client.loop_forever()

    def on_connect(self):
        print("Connected to MQTT server.")
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'terminal')
        self.broadcast_msg('terminal', '{} with id {} has connected to the server.'.format('Primary' if self.role == Role.PRIMARY else 'Replica', self.id))
        self.broadcast_msg('assign_id', json.dumps({'id': self.id}))

        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'client_req')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'client_req', self.on_client_message)

        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'pre-prepare')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'pre-prepare', self.on_pre_prepare_message)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'prepare')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'prepare', self.on_prepare_message)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'commit')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'commit', self.on_commit_message)

    def on_message(self, msg):
        pass

    def on_client_message(self, msg):
        print('on_client_message', msg)

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

                # Initiate log and append pre-prepare message
                self.initiate_log()
                self.log[self.current_seq]['pre-prepare'][self.id] = pre_prepare_msg

                self.broadcast_msg('pre-prepare', pre_prepare_msg)
                self.requests += 1
        else: # Get into Waiting state
            if self.state == State.READY:
                self.state == State.WAITING
                self.timer.run()

    def on_pre_prepare_message(self, msg):
        print('on_pre_prepare_message', msg)

        if self.state == State.CHANGING:
            return
        if self.state == State.WAITING:
            self.timer.reset()
            self.timer.run()
        if self.role == Role.REPLICA:
            pre_prepare_msg = self.validate_proposal(msg)
            if pre_prepare_msg and self.current_phase == ConsensusPhase.IDLE:
                # Update local client request info
                self.client_req = pre_prepare_msg['m']
                self.client_req_digest = pre_prepare_msg['d']
                self.client_req_dict = json.loads(self.client_req)

                # Update sequence number and consensus phase
                self.current_seq = pre_prepare_msg['n']
                self.current_phase = ConsensusPhase.PREPARE

                # Append pre-prepare message to log
                self.initiate_log()
                self.log[self.current_seq]['pre-prepare'][pre_prepare_msg['i']] = msg

                # Reject request with lower timestamp than last replied request to ensure exactly-once semantics
                try:
                    if self.client_req_dict['t'] <= self.reply_history[self.client_req_dict['c']]['t']:
                        return
                except KeyError:
                    pass

                # Construct and broadcast the prepare message
                prepare_msg = self.construct_msg()
                self.broadcast_msg('prepare', prepare_msg)
                self.requests += 1
        else:
            self.current_phase = ConsensusPhase.PREPARE

    def on_prepare_message(self, msg):
        print('on_prepare_message', msg)

        if self.state == State.CHANGING:
            return
        prepare_msg = self.validate_msg(msg)
        if prepare_msg and self.current_phase == ConsensusPhase.PREPARE:
            # Append prepare message to log
            self.log[self.current_seq]['prepare'][prepare_msg['i']] = msg

            if len(self.log[self.current_seq]['prepare'].keys()) + self.role.value > 2 * FAULT_TOLERANCE:
                # Update consensus phase
                self.current_phase = ConsensusPhase.COMMIT

                # Construct and broadcast the prepare message
                commit_msg = self.construct_msg()
                self.broadcast_msg('commit', commit_msg)

    def on_commit_message(self, msg):
        print('on_commit_message', msg)

        if self.state == State.CHANGING:
            return
        commit_msg = self.validate_msg(msg)
        if commit_msg['i'] == self.current_view:
            self.timer.reset()
            self.timer.run()
        if commit_msg and self.current_phase == ConsensusPhase.COMMIT:
            # Append commit message to log
            self.log[self.current_seq]['commit'][commit_msg['i']] = msg

            if len(self.log[self.current_seq]['commit'].keys()) > 2 * FAULT_TOLERANCE:
                result  = self.execute(self.client_req_dict['o'])
                if result:
                    # Update consensus phase
                    self.current_phase = ConsensusPhase.IDLE

                    # Construct and broadcast the prepare message as well as appending it to log
                    reply_msg = self.construct_reply('Request executed succeeded' if result else 'Request executed failed')
                    # Append the reply message to reply history
                    self.append_reply_history(reply_msg)
                    self.broadcast_msg('reply', reply_msg)

    def execute(self, operation) -> bool:
        with open('log_sample/log_{}.json'.format(self.id), 'w', encoding='utf-8') as fw:
            json.dump(self.log, fw)
        self.requests -= 1
        if self.requests == 0:
            self.state = State.READY
        return True

    def broadcast_msg(self, topic, msg):
        print('broadcast', topic, msg)
        self.mqtt_client.publish(MQTT_TOPIC_PREFIX + topic, msg)

    def initiate_log(self):
        self.log[self.current_seq] = {}
        self.log[self.current_seq]['v'] = self.current_view
        self.log[self.current_seq]['pre-prepare'] = {}
        self.log[self.current_seq]['prepare'] = {}
        self.log[self.current_seq]['commit'] = {}

    def append_reply_history(self, msg):
        self.reply_history[self.client_req_dict['c']] = {}
        self.reply_history[self.client_req_dict['c']]['t'] = self.client_req_dict['t']
        self.reply_history[self.client_req_dict['c']]['r'] = msg

    # {"o": "", "t": "", "c": ""}
    def validate_client_req(self, msg) -> dict:
        try:
            msg = json.loads(msg)
            if not all (key in msg for key in ['o', 't', 'c']):
                msg = {}
            return msg
        except (json.decoder.JSONDecodeError, KeyError) as e:
            print('error', e)
            return {}

    def validate_proposal(self, msg) -> dict:
        try:
            msg = json.loads(msg)
            val_mac = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in MSG_DIGEST_KEYS}), msg['i'])
            val_digest = self.get_digest(msg['m'])
            if not (msg['mac'] == val_mac and msg['d'] == val_digest and msg['v'] == self.current_view and msg['n'] > self.current_seq):
                msg = {}
            return msg
        except (json.decoder.JSONDecodeError, KeyError):
            return {}

    def validate_msg(self, msg) -> dict:
        try:
            msg = json.loads(msg)
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
        # Encoding content using md5 hash
        content = hashlib.md5(content.encode())
        # Return as a str object
        content = content.hexdigest()
        return content
        # In case of using only the 10 least bytes as the paper did, comment out previous line, and uncomment following one
        # return content[:10]

    def get_mac(self, msg, key) -> string:
        # mac = md5(msg, key)
        # Encoding msg using md5 hash
        msg = hashlib.md5(msg.encode())
        # Update msg with key
        msg.update(str(key).encode())
        # Return as a str object
        msg = msg.hexdigest()
        return msg
        # In case of using only the 10 least bytes as the paper did, comment out previous line, and uncomment following one
        # return msg[:10]

    def update_role(self): # Can be used in view change to update a replica's role
        leader_id = self.current_view % NODE_TOTAL_NUMBER
        self.role = Role.PRIMARY if self.id == leader_id else Role.REPLICA
        self.broadcast_msg('view_change', json.dumps({'leader_id': leader_id}))


    def init_view_change(self):
        if self.state == State.WAITING:
            self.state = State.CHANGING
            self.current_phase = ConsensusPhase.VIEWCHANGE
            # send view-change message
            self.current_view += 1
            self.update_role()


if __name__ == '__main__':
    replica_id = int(sys.argv[1])
    print('replica_id', replica_id)
    primary = Replica(replica_id=replica_id)
