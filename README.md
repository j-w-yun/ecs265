# ecs265

## Test Method
* Open MQTT Explorer, choose test.mosquitto.org on the left side of the connection page and change the topic to `ecs265/pbft/#`
* run four instances of replica by using the following command with replica_id from 0 to 3 (You should see messages in `ecs265/pbft/terminal`)
  ```
  python replica.py replica_id
  ```
* Publish a client request with topic `ecs265/pbft/client_req` with the following format
  ```
  {"o":"Operation requested","t":1637563592,"c":0}
  ```
* You should be able to see messages of different topics and everyone finally commits and replies 
## Message Types

As messages with different types are sent into different topics, there is no need to contain the message type in the message content.

* **Client** request: 
  * o: operation
  * t: timestamp
  * c: client id
* **Pre-prepare** message:
  * v: view number
  * n: sequence number
  * d: digest of client message
  * i: replica id
  * mac: message authentication code
  * m: client message
* **Prepare** message:
  * v: view number
  * n: sequence number
  * d: digest of client message
  * i: replica id
  * mac: message authentication code
* **Commit** message:
  * v: view number
  * n: sequence number
  * d: digest of client message
  * i: replica id
  * mac: message authentication code
* **Reply** message:
  * v: view number
  * t: timestamp of the client request
  * c: client id
  * i: replica id
  * r: execution result

## TODO
  * Code review for timer and state lock
  * Code review for digest and MAC generation function
