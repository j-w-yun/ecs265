# PBFT Dashboard
1, Download the code.

2, Open the terminal/command line and navigate to the folder of the code.

3, Install specific libraries:

```
pip3 uninstall "python-socketio[client]"
pip3 install "python-socketio[client]"==5.3.0
```

4, Set target size of network in `global_const.py`:

```
NODE_TOTAL_NUMBER = 4
```

5, Run replicas:
In the path of the folder of the code, open terminal/command-line interfaces with the number of NODE_TOTAL_NUMBER.
In the first interface, run "python replica.py 0", as the number 0 indicates it is running node 0.
In the second interface, run "python replica.py 1", as the number 1 indicates it is running node 1.
Etc.

```
python replica.py 0
...
python replica.py 3
```

6, PBFT Dashboard:
Go to https://jaewan-yun.com/project/pbft press the ">" button at the center of the bottom to send the client message. See the visualization result in the top left corner.

7, Rules and View change:
Use the "Add Rule" button to add rules. Select a feature in the first block and phase in the second block, put the node number you want to directly affect in the "Source" blank, and "Target" is the node receiving the message from the directly affected node. "Value" can be used to give a value of latency.
Press "Apply Rules" after entering rules.
When needed view change, press the "Trigger View Change" button. The visualization part will show the change of the primary node.

8, To change the total number of replicas, change the target size of the network in step 4 and then run a specific number of replicas in step 5.
