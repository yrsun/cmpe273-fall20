import zmq
import time
import sys
import requests
from itertools import cycle
from consistent_hashing import Ring
from hrw import HRW

manager_conn = 0

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.REQ)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers):
    print("Starting Round Robin...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        curPool = next(pool)
        print(f"Sending data:{data} to {servers[num % 4]}")
        curPool.send_json(data)
        message = curPool.recv_json()
    time.sleep(1)
    print("Done")


def generate_data_consistent_hashing(servers):
    print("Starting Consistent Hashing...")

    print("1. Add data one by one to four servers...")
    producers = create_clients(servers)
    ring = Ring(servers)
    for num in range(10, 20):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        curPool = producers[ring.get_node(num)]
        print(f"Sending data:{data} to {ring.get_node(num)}") 
        curPool.send_json(data)
        message = curPool.recv_json()
    time.sleep(1)

    print("2. Get data one by one from four servers...")
    for num in range(10, 20):
        data = {'op': 'GET_ONE', 'key': f'key-{num}'}
        curPool = producers[ring.get_node(num)]
        curPool.send_json(data)
        message = curPool.recv_json()
        print(f"Getting data:{message} from {ring.get_node(num)}")
    time.sleep(1)

    print("3. Get All data from four servers...")
    for p in producers.values():
        data = {'op': 'GET_ALL'}
        p.send_json(data)
        message = p.recv_json()
        s = list(producers.keys())[list(producers.values()).index(p)]
        print(f"Getting data:{message} from {s}")
    time.sleep(1)

    # del Node
    print("4. Delete one node...")
    delPortOld = servers[0]
    delPortNew = ring.del_node(delPortOld)

    print(f"Delete {delPortOld}...")
    producers[delPortOld].send_json({'op': 'GET_ALL'})
    data = producers[delPortOld].recv_json()
    for d in data['collection']:
        producers[delPortNew].send_json({'op': 'PUT', 'key': d['key'], 'value': d['value']})
        message = producers[delPortNew].recv_json()
    producers[delPortOld].send_json({'op': 'CLEAN'})
    message = producers[delPortOld].recv_json()

    print(f"Datastore deleted from {delPortOld}...")
    producers[delPortOld].send_json({'op': 'GET_ALL'})
    print(producers[delPortOld].recv_json())
    print(f"Datastore added to {delPortNew}...")
    producers[delPortNew].send_json({'op': 'GET_ALL'})
    print(producers[delPortNew].recv_json())

    print("Sent DEL NODE to server manager. Check servers from server and consul...")
    manager_conn.send_json({'op': 'REMOVE', 'delPortOld': f'{delPortOld}', 'delPortNew': f'{delPortNew}'})
    message = manager_conn.recv_json()
    time.sleep(1)

    # add Node
    print("5. Add one node...")
    addPortNew = 'tcp://127.0.0.1:2001'
    addPortOld = ring.add_node(addPortNew)

    print(f"Add {addPortNew}..")
    print("Sent ADD NODE to server manager. Check servers from server and consul...")
    manager_conn.send_json({'op': 'ADD', 'addPortNew': f'{addPortNew}', 'addPortOld': f'{addPortOld}'})
    message = manager_conn.recv_json()

    print(f"Rebalance data from {addPortOld} to {addPortNew}...")
    producers[addPortOld].send_json({'op': 'GET_ALL'})
    data = producers[addPortOld].recv_json()
    producers[addPortOld].send_json({'op': 'CLEAN'})
    message = producers[addPortOld].recv_json()
    for d in data['collection']:
        num = int(d['key'].split('-')[1])
        producers[ring.get_node(num)].send_json({'op': 'PUT', 'key': d['key'], 'value': d['value']})
        message = producers[ring.get_node(num)].recv_json()

    print(f"Datastore from {addPortOld}...")
    producers[addPortOld].send_json({'op': 'GET_ALL'})
    print(producers[addPortOld].recv_json())
    print(f"Datastore from {addPortNew}...")
    producers[addPortNew].send_json({'op': 'GET_ALL'})
    print(producers[addPortNew].recv_json())

    print("Try to Get One key to make sure kv stored correctly...")
    print(f"Get 17 from {ring.get_node(17)}...")
    producers[ring.get_node(17)].send_json({'op': 'GET_ONE', 'key': 'key-17'})
    print(producers[ring.get_node(17)].recv_json())
    print(f"Get 12 from {ring.get_node(12)}...")
    producers[ring.get_node(12)].send_json({'op': 'GET_ONE', 'key': 'key-12'})
    print(producers[ring.get_node(12)].recv_json())
    time.sleep(1)
    print("Done")

def generate_data_hrw_hashing(servers):
    print("Starting HRW hashing...")
    producers = create_clients(servers)
    hrw_hashing = HRW(servers)
    for num in range(20, 30):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        curPool = producers[hrw_hashing.get_node(num)]
        print(f"Sending data:{data} to {hrw_hashing.get_node(num)}") 
        curPool.send_json(data)
        message = curPool.recv_json()
    time.sleep(1)
    print("Done")

if __name__ == "__main__":
    URL = "http://127.0.0.1:8500/v1/agent/services";
    r = requests.get(url = URL)
    data = r.json()
    servers = []
    manager_port = 0
    print(f"num_server={len(data)}")
    for k in data:
        if data[k]['Tags'][0] == 'worker':
            server_port = data[k]['Port']
            servers.append(f'tcp://127.0.0.1:{server_port}')
        else:
            manager_port = data[k]['Port']
    context = zmq.Context()
    manager_conn = context.socket(zmq.REQ)
    print(f"Creating a manager connection to {manager_port}")
    manager_conn.bind(f'tcp://127.0.0.1:{manager_port}')

    print("Servers:", servers)
    generate_data_round_robin(servers)
    generate_data_hrw_hashing(servers)
    generate_data_consistent_hashing(servers)
