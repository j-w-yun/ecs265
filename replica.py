import global_const

REPLICA_NUMBER = -1


def broadcast_to_all_nodes(message):
    print("Broadcasting to All nodes: "+ str(message))
    global_const.mqttStuff.publish(global_const.topicPrefix + "/prepare", message + " REPLICA "+str(REPLICA_NUMBER))

def get_message(topic, message):
    ## REQUEST PHASE
    if topic == global_const.topicPrefix + "/request":
        print("Request message: "+str(message)+" received from client. Replica " + str(REPLICA_NUMBER) + " does nothing.")

    ## PRE-PREPARE PHASE
    if topic == global_const.topicPrefix + "/pre-prepare":
        print("Pre-prepare message received. Replica " + str(REPLICA_NUMBER) + " is now in PREPARE PHASE. \nBroadcasting PREPARE to all")
        input("Press Enter to continue...")
        broadcast_to_all_nodes(str(message))

    ## PREPARE PHASE
    if topic == global_const.topicPrefix + "/prepare":
        print("Prepare message received from a different Node: "+str(message))

def init_replica(replica_id):
    global REPLICA_NUMBER
    REPLICA_NUMBER = replica_id 
    print("Replica " + str(REPLICA_NUMBER) + " initialized.")

print("")