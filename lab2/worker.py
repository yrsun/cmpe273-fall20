import zmq
import sys

context = zmq.Context()

sock1 = context.socket(zmq.PULL)
sock1.connect("tcp://127.0.0.1:1234")
sock2 = context.socket(zmq.PUSH)
sock2.bind("tcp://127.0.0.1:1235")

while True:
    message = sock1.recv().decode()
    msg = float(message) ** 0.5
    sock2.send(str(msg).encode())
    #sock2.recv()
    #sock1.send(message)
    print(message)
