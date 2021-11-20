from enum import Enum


MQTT_SERVER_ADDR = 'test.mosquitto.org'
MQTT_SERVER_PORT = 1883
MQTT_TOPIC_PREFIX = 'ecs265/pbft/'

FAULT_TOLERANCE = 1

class ConsensusPhase(Enum):
    IDLE = 0
    PRE_PREPARE = 1
    PREPARE = 2
    COMMIT = 3
    VIEWCHANGE = 4

class Role(Enum):
    REPLICA = 0
    PRIMARY = 1
