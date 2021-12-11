import asyncio
import hashlib
import json
import string
import sys

from threading import Timer


# import paho.mqtt.client as mqtt

from global_const import *




class Client:
    def __init__(self, namespace='/pbft'):
        self.namespace = namespace

        self.host = None
        self.port = None
        self.sio = None

        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None

        self.topics = dict()

    def subscribe(self, topic):
        self.topics[topic] = None

    def message_callback_add(self, topic, callback):
        self.topics[topic] = callback

    def publish(self, topic, message):
        self.sio.emit(topic, message, namespace=self.namespace)

    def _initialize_socket(self):
        client = self

        @self.sio.event(namespace=self.namespace)
        def connect():
            if self.on_connect:
                self.on_connect()

        @self.sio.event(namespace=self.namespace)
        def disconnect():
            if self.on_disconnect:
                self.on_disconnect()

        class Namespace(socketio.ClientNamespace):
            def trigger_event(self, name, *args):
                # print(f"event={name}, payload={args}")
                if name in client.topics and client.topics[name]:
                    client.topics[name](*args)

        self.sio.register_namespace(Namespace(self.namespace))

    def connect(self, host, port):
        self.sio = socketio.Client()
        self._initialize_socket()

        self.host = host
        self.port = port
        self.sio.connect(f"{self.host}:{self.port}", namespaces=[self.namespace])

    def loop_forever(self):
        while True:
            time.sleep(1)


class WaitTimer:

    def __init__(self, timeout, func):
        self.timeout = timeout
        self.func = func

        self.timer = None

    def callback(self):
        self.func()

    def cancel(self):
        self.timer.cancel()

    def start(self):
        self.timer = Timer(self.timeout, self.callback)
        self.timer.start()


class Replica:
    def __init__(self, replica_id=0, current_seq=0, current_view=0, timeout=20) -> None:
        self.id = replica_id
        self.current_seq = current_seq
        self.current_view = current_view
        self.timeout = timeout
        self.timer = WaitTimer(timeout, self.init_view_change)
        self.role = Role.REPLICA

        self.current_phase = ConsensusPhase.IDLE
        self.request_waitlist = []
        self.log = {}
        self.reply_history = {}
        self.vc_history = {}

        self.client_req = ''
        self.client_req_digest = ''
        self.client_req_dict ={}

        # MQTT client setup
        self.mqtt_client = Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.connect(MQTT_SERVER_ADDR, MQTT_SERVER_PORT)

        # Update role 
        self.update_role()

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

        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'reset_history')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'reset_history', self.on_reset_history)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'view_change')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'view_change', self.on_trigger_view_change)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'view_change_auto')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'view_change_auto', self.on_view_change_message)
        self.mqtt_client.subscribe(MQTT_TOPIC_PREFIX + 'new_view')
        self.mqtt_client.message_callback_add(MQTT_TOPIC_PREFIX + 'new_view', self.on_new_view_message)

    def on_message(self):
        pass

    def on_trigger_view_change(self):
        print('trigger view change')

        self.on_reset_history()
        self.current_view += 1
        self.update_role()

    def on_reset_history(self):
        print('reset history')

        self.current_seq = 0
        self.current_view = 0
        try:
            self.timer.cancel()
        except AttributeError:
            pass
        self.timer = WaitTimer(self.timeout, self.init_view_change)
        self.request_waitlist = []

        # self.requests = 0
        self.role = Role.REPLICA

        self.current_phase = ConsensusPhase.IDLE
        self.log = {}
        self.reply_history = {}

        self.client_req = ''
        self.client_req_digest = ''
        self.client_req_dict ={}

        self.update_role()

    def on_client_message(self, msg):
        print('on_client_message', msg)

        if self.validate_client_req(msg): # Avoid triggering view change when client request is corrupted
            self.request_waitlist.append(msg)

        if self.current_phase == ConsensusPhase.IDLE:
            self.current_phase = ConsensusPhase.WAIT
            if self.role == Role.REPLICA:
                self.timer.start()

        if self.role == Role.PRIMARY and self.current_phase == ConsensusPhase.WAIT:
            msg = self.request_waitlist[0]

            self.client_req_dict = self.validate_client_req(msg)

            # Update local client request info
            self.client_req = msg
            self.client_req_digest = self.get_digest(self.client_req)

            # Update sequence number and consensus phase
            self.current_seq += 1
            self.current_phase = ConsensusPhase.PREPARE

            # Construct and broadcast the pre-prepare message as well as appending it to log
            pre_prepare_msg = self.construct_msg(if_proposal=True)

            # Initiate log and append pre-prepare message
            self.initiate_log()
            self.log[self.current_seq]['pre-prepare'][self.id] = pre_prepare_msg

            self.broadcast_msg('pre-prepare', pre_prepare_msg)

    def on_pre_prepare_message(self, msg):
        print('on_pre_prepare_message', msg)

        if self.role == Role.REPLICA:
            pre_prepare_msg = self.validate_proposal(msg)
            if pre_prepare_msg and self.current_phase == ConsensusPhase.WAIT:
                # Update local client request info
                self.client_req = pre_prepare_msg['m']
                self.client_req_digest = pre_prepare_msg['d']
                self.client_req_dict = json.loads(self.client_req)

                # Reject request with lower timestamp than last replied request to ensure exactly-once semantics
                try:
                    if self.client_req_dict['t'] <= self.reply_history[self.client_req_dict['c']]['t']:
                        return
                except KeyError:
                    pass

                # Update sequence number and consensus phase
                self.current_seq = pre_prepare_msg['n']
                self.current_phase = ConsensusPhase.PREPARE

                # Append pre-prepare message to log
                self.initiate_log()
                self.log[self.current_seq]['pre-prepare'][pre_prepare_msg['i']] = msg

                # Construct and broadcast the prepare message
                prepare_msg = self.construct_msg()
                self.broadcast_msg('prepare', prepare_msg)

    def on_prepare_message(self, msg):
        print('on_prepare_message', msg)

        prepare_msg = self.validate_msg(msg)
        if prepare_msg and self.current_phase == ConsensusPhase.PREPARE:
            # Append prepare message to log
            self.log[self.current_seq]['prepare'][prepare_msg['i']] = msg

            if len(self.log[self.current_seq]['prepare'].keys()) >= 2 * FAULT_TOLERANCE:
                # Update consensus phase
                self.current_phase = ConsensusPhase.COMMIT

                # Construct and broadcast the prepare message
                commit_msg = self.construct_msg()
                self.broadcast_msg('commit', commit_msg)

    def on_commit_message(self, msg):
        print('on_commit_message', msg)

        commit_msg = self.validate_msg(msg)
        if commit_msg and self.current_phase == ConsensusPhase.COMMIT:
            # Append commit message to log
            self.log[self.current_seq]['commit'][commit_msg['i']] = msg

            if len(self.log[self.current_seq]['commit'].keys()) > 2 * FAULT_TOLERANCE:
                result  = self.execute(self.client_req_dict['o'])
                if result:

                    # Construct and broadcast the prepare message as well as appending it to log
                    reply_msg = self.construct_reply('Request execution successful' if result else 'Request execution failed')
                    # Append the reply message to reply history
                    if self.client_req_dict['t'] > 0:
                        self.append_reply_history(reply_msg)
                        self.broadcast_msg('reply', reply_msg)
                        self.request_waitlist.remove(self.client_req)

                    # Update consensus phase
                    if self.role == Role.REPLICA: # Current request committed, reset timer
                        try:
                            self.timer.cancel()
                        except AttributeError:
                            pass
                    if len(self.request_waitlist) == 0:
                        self.current_phase = ConsensusPhase.IDLE # No request in waitlist, timer stop
                    else:
                        self.current_phase = ConsensusPhase.WAIT # Still have requests in waitlist
                        if self.role == Role.PRIMARY:
                            self.on_client_message('')
                        else: # Replica timer restart
                            self.timer.start()

    def on_view_change_message(self, msg):
        print('view change request received')

        # Validate view change message and checking if current node is next primary
        view_change_msg = self.validate_view_change_msg(msg)

        # add vote to vc_history if current node is next primary for the view change request
        if view_change_msg and view_change_msg['i'] != self.id:
            self.vc_history[view_change_msg['i']] = view_change_msg

            if len(self.vc_history.keys()) >= 2 * FAULT_TOLERANCE: # Check if any view has enough votes
                self.current_phase = ConsensusPhase.VIEW_CHANGE
                self.current_view = view_change_msg['v']
                self.update_role() # Changed to primary, cancel timer
                try:
                    self.timer.cancel()
                except AttributeError:
                    pass

                # Create new view message and append pre-prepare message to log
                new_view_msg, pre_prepare_msg = self.construct_new_view_msg()
                self.initiate_log()
                self.log[self.current_seq]['pre-prepare'][self.id] = pre_prepare_msg

                # Update client_req, client_req_digest and self.client_req_dict as in on_client_request_message()
                pre_prepare_msg_dict = json.loads(pre_prepare_msg)
                self.client_req = pre_prepare_msg_dict['m']
                self.client_req_digest = pre_prepare_msg_dict['d']
                try:
                    self.client_req_dict = json.loads(self.client_req)
                except json.decoder.JSONDecodeError: # client message and digest is empty, filling the gap
                    self.client_req_dict = {'o': '', 't': -1, 'c': -1}

                self.broadcast_msg('new_view', new_view_msg)
                self.vc_history = {} # cleared to avoid resending
                self.current_phase = ConsensusPhase.PREPARE

    def on_new_view_message(self, msg):
        print('new view established')

        new_view_msg = self.validate_new_view_msg(msg)
        if new_view_msg and (new_view_msg['v'] % NODE_TOTAL_NUMBER) != self.id:

            try:
                self.timer.cancel() # Stop timer as entering new view
            except AttributeError:
                pass

            self.vc_history = {}
            self.current_view = new_view_msg['v']
            self.current_seq = new_view_msg['n']
            self.update_role() # Seems unnecessary

            self.initiate_log()
            self.log[self.current_seq]['pre-prepare'][new_view_msg['i']] = new_view_msg['O']

            # Update client_req, client_req_digest and self.client_req_dict as in on_pre_prepare_message()
            pre_prepare_msg = json.loads(new_view_msg['O'])
            self.client_req = pre_prepare_msg['m']
            self.client_req_digest = pre_prepare_msg['d']
            try:
                self.client_req_dict = json.loads(self.client_req)
            except json.decoder.JSONDecodeError: # client message and digest is empty, filling the gap
                self.client_req_dict = {'o': '', 't': -1, 'c': -1}

            # Construct and broadcast the prepare message
            prepare_msg = self.construct_msg()
            self.broadcast_msg('prepare', prepare_msg)
            self.current_phase = ConsensusPhase.PREPARE

            self.timer.start() # Restart

    def execute(self, operation) -> bool:
        with open('log_sample/log_{}.json'.format(self.id), 'w', encoding='utf-8') as fw:
            json.dump(self.log, fw)
        return True

    def init_view_change(self):
        self.current_phase = ConsensusPhase.VIEW_CHANGE
        self.current_view += 1 # current_view is iterated

        # Start view change timer
        try:
            self.timer.cancel()
        except AttributeError:
            pass
        self.timer.start()
        
        # send view-change message
        msg = self.construct_view_change_msg()
        self.broadcast_msg('view_change_auto', msg)

    def broadcast_msg(self, topic, msg):
        # print(f'Topic: {topic}')
        # print(f'Msg: {msg}')
        # print('-----------------------')
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
        except (json.decoder.JSONDecodeError, KeyError):
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

    def validate_view_change_msg(self, msg) -> dict:

        def validate_P(P):
            return True

        try:
            msg = json.loads(msg)
            val_mac = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in VIEW_CHANGE_MSG_DIGEST_KEYS}), msg['i'])
            if  not (msg['mac'] == val_mac and (msg['v'] % NODE_TOTAL_NUMBER) == self.id and validate_P(msg['P'])):
                msg = {}
            return msg
        except (json.decoder.JSONDecodeError, KeyError):
            return {}

    def validate_new_view_msg(self, msg) -> dict:

        def validate_V(v, V):
            return True

        def validate_O(msg):
            return True
        try:
            msg = json.loads(msg)
            val_mac = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in NEW_VIEW_MSG_DIGEST_KEYS}), msg['i'])
            if  not (msg['mac'] == val_mac and (msg['v'] % NODE_TOTAL_NUMBER) == msg['i'] and validate_V(msg['v'], msg['V']) and validate_O(msg)):
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

    def construct_view_change_msg(self) -> string:
        msg = {}
        msg['v'] = self.current_view
        msg['n'] = self.current_seq

        # Get uncommitted requests
        if self.current_seq in self.log and len(self.log[self.current_seq]['prepare'].keys()) >= 2 * FAULT_TOLERANCE and len(self.log[self.current_seq]['commit'].keys()) <= 2 * FAULT_TOLERANCE:
            msg['P'] = {'pre-prepare': self.log[self.current_seq]['pre-prepare'], 'prepare': self.log[self.current_seq]['prepare']}
        else:
            msg['P'] = {'pre-prepare': {0: ''}}

        msg['i'] = self.id
        msg['mac'] = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in VIEW_CHANGE_MSG_DIGEST_KEYS}), self.id)

        return json.dumps(msg)

    def construct_prepare_msg_for_view_change(self) -> string:
        max_seq = 0
        resend = ''
        pre_prepare_msg = ''
        for _, vc_msg in self.vc_history.items():
            if vc_msg['n'] > max_seq and vc_msg['P']:
                max_seq = vc_msg['n']
                pre_prepare_msg = list(vc_msg['P']['pre-prepare'].values())[0]
        
        self.current_seq = max_seq # Actually not appropriate to do it here
        try:
            pre_prepare_msg = json.loads(pre_prepare_msg)
            d, m = pre_prepare_msg['d'], pre_prepare_msg['m']
        except (json.decoder.JSONDecodeError, KeyError):
            d, m = '', "{'o': '', 't': -1, 'c': -1}"

        msg = {}
        msg['v'] = self.current_view
        msg['n'] = self.current_seq
        msg['d'] = d
        msg['i'] = self.id
        msg['mac'] = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in MSG_DIGEST_KEYS}), self.id) # Currently using replica id as the parameter for key in the get_mac()
        msg['m'] = m
        resend = json.dumps(msg)

        return resend

    def construct_new_view_msg(self):
        msg = {}
        msg['v'] = self.current_view
        msg['V'] = self.vc_history
        msg['O'] = self.construct_prepare_msg_for_view_change()
        msg['n'] = self.current_seq
        msg['i'] = self.id
        msg['mac'] = self.get_mac(json.dumps({key : value for key, value in msg.items() if key in NEW_VIEW_MSG_DIGEST_KEYS}), self.id)

        return json.dumps(msg), msg['O']

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
        print('replica', self.id, 'role', self.role)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Please provide an ID for the replica')
        sys.exit(1)

    # print('replica_id', replica_id)
    Replica(replica_id=int(sys.argv[1]))
