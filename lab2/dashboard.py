import zmq
import sys
import time

context = zmq.Context()

sock = context.socket(zmq.PULL)
sock.bind("tcp://127.0.0.1:1235")

for i in range(10001):
    msg = sock.recv().decode()
    print(msg)
