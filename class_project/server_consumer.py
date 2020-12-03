import zmq
import json
import requests
import sys
import os
from  multiprocessing import Process
import consul

process_q = {}

def server(port):
    context = zmq.Context()
    consumer = context.socket(zmq.REP)
    consumer.connect(f"tcp://127.0.0.1:{port}")
    datastore = {}

    while True:
        raw = consumer.recv_json()
        print(f"Server_port={port} received {raw}")
        op = raw['op']
        if op == 'PUT':
            key, value = raw['key'], raw['value']
            datastore[key] = value
            res = {'response': 'success'}
            consumer.send_json(res)
        elif op == 'GET_ONE':
            key = raw['key']
            value = datastore[key]
            res = {'key': f'{key}', 'value': f'{value}'}
            consumer.send_json(res)
        elif op == 'GET_ALL':
            collection = []
            for k in datastore.keys():
                v = datastore[k]
                data = {'key': f'{k}', 'value': f'{v}'}
                collection.append(data)
            res = {'collection': collection}
            consumer.send_json(res)
        elif op == 'CLEAN':
            datastore = {}
            consumer.send_json({'response': 'success'})


if __name__ == "__main__":
    URL = "http://127.0.0.1:8500/v1/agent/services";
    r = requests.get(url = URL)
    data = r.json()
    print(data)
    manager_port = 0
    for k in data:
        if data[k]['Tags'][0] == 'worker':
            server_port = data[k]['Port']
            print(f"Starting a server at:{server_port}...")
            process = Process(target=server, args=(server_port,))
            process_q[server_port] = process
            process.start()
            print(process.pid)
        else:
            manager_port = data[k]['Port']
    context = zmq.Context()
    manager_conn = context.socket(zmq.REP)
    print(f"Starting a manager at:{manager_port}...")
    manager_conn.connect(f"tcp://127.0.0.1:{manager_port}")
    while True:
        raw = manager_conn.recv_json()
        print(f"Manager received {raw}...")
        op = raw['op']
        if op == 'REMOVE':
            delPortOld = raw['delPortOld']
            delPortNew = raw['delPortNew']
            URLD = "http://127.0.0.1:8500/v1/agent/service"
            r = requests.put(URLD+"/deregister/server"+f'{delPortOld[-1]}')
            r = requests.get(url = URL)
            data = r.json()
            print(f"Manager killed {delPortOld}...")
            print(data)
            process_q[int(delPortOld[-4:])].terminate()
            manager_conn.send_json({'res': 'success'})
        elif op == 'ADD':
            addPortNew = raw['addPortNew']
            addPortOld = raw['addPortOld']
            URLI = "http://127.0.0.1:8500/v1/agent/service"
            data = {"address": "127.0.0.1", "name": "server1", "tags": ["worker"], "port": int(addPortNew[-4:])}
            r = requests.put(URLI+"/register", json=data)
            print(f"Manager added {addPortNew}...")
            print(requests.get(url = URL).json())
            process = Process(target=server, args=(int(addPortNew[-4:]),))
            process_q[int(addPortNew[-4:])] = process
            process.start()
            manager_conn.send_json({'res': 'success'})

