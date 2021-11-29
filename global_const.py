from enum import Enum


# MQTT_SERVER_ADDR = 'test.mosquitto.org'
# MQTT_SERVER_PORT = 1883
# MQTT_TOPIC_PREFIX = 'ecs265/pbft/'
MQTT_SERVER_ADDR = 'https://jaewan-yun.com'
MQTT_SERVER_PORT = '5000'
MQTT_TOPIC_PREFIX = ''

NODE_TOTAL_NUMBER = 4
FAULT_TOLERANCE = 1

MSG_DIGEST_KEYS = ['i', 'n', 'v', 'd']

class ConsensusPhase(Enum):
    IDLE = 0
    PRE_PREPARE = 1
    PREPARE = 2
    COMMIT = 3
    VIEWCHANGE = 4
    NEW_VIEW = 5

class Role(Enum):
    REPLICA = 0
    PRIMARY = 1

class State(Enum):
    READY = 0
    WAITING = 1
    CHANGING = 2