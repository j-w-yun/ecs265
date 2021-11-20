# ecs265

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

* digest generation function
* MAC generation function
* Currently the replica cannot reject a request with a timestamp older than a former request
* Current log is just a list and can be changed to dict according to the need of view change