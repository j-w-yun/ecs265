import asyncio
import socketio
import time


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
                print(f"event={name}, payload={args}")
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
            # print('loop')
