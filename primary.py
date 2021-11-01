import global_const


def broadcast_to_replicas(message):
    print("Broadcasting to Replicas: "+ str(message))
    global_const.current_phase = "PRE-PREPARE"
    global_const.mqttStuff.publish(global_const.topicPrefix + "/pre-prepare", message)


def get_message(topic, message):
    ## REQUEST PHASE
    if topic == global_const.topicPrefix + "/request":
        print("Request message received from client. Primary broadcasts to all replicas.")
        input("Press Enter to continue...")
        broadcast_to_replicas(message)

    ## PRE-PREPARE PHASE
    if topic == global_const.topicPrefix + "/preprepare":
        print("Pre-prepare message received. Primary does nothing")



    


    