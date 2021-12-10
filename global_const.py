from enum import Enum


# MQTT_SERVER_ADDR = 'test.mosquitto.org'
# MQTT_SERVER_PORT = 1883
# MQTT_TOPIC_PREFIX = 'ecs265/pbft/'
MQTT_SERVER_ADDR = 'https://jaewan-yun.com'
MQTT_SERVER_PORT = '5000'
MQTT_TOPIC_PREFIX = ''

NODE_TOTAL_NUMBER = 5
FAULT_TOLERANCE = 1

MSG_DIGEST_KEYS = ['i', 'n', 'v', 'd']
VIEW_CHANGE_MSG_DIGEST_KEYS = ['v', 'n', 'P', 'i']
NEW_VIEW_MSG_DIGEST_KEYS = ['v', 'V', 'O', 'i']

class ConsensusPhase(Enum):
    IDLE = 0
    PRE_PREPARE = 1
    PREPARE = 2
    COMMIT = 3
    WAIT = 4
    VIEW_CHANGE = 5
    NEW_VIEW = 6

class Role(Enum):
    REPLICA = 0
    PRIMARY = 1