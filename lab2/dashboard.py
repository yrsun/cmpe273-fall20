import zmq
import sys

context = zmq.Context()

sock = context.socket(zmq.PULL)
sock.connect("tcp://127.0.0.1:1235")
#sock.connect("tcp://127.0.0.1:1236")
#sock.connect("tcp://127.0.0.1:1237")

while True:
    msg = sock.recv().decode()
    #sock.send(msg.encode())
    print(msg)
