import zmq
import sys
import time

context = zmq.Context()

sock = context.socket(zmq.PUSH)
sock.bind("tcp://127.0.0.1:1234")

time.sleep(1)
for i in range(10001):
    sock.send(str(i).encode())
