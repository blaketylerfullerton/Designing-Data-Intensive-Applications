import socket
import json
import threading
import time
from node import PNCounterNode
from network import NetworkSimulator, PartitionManager

def send_request(host, port, request, timeout=2.0):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.sendall(json.dumps(request).encode())
        response = json.loads(sock.recv(65536).decode())
        sock.close()
        return response
    except Exception as e:
        return {'ok': False, 'error': str(e)}

def get_counter(port):
    return send_request('localhost', port, {'cmd': 'get'})

def increment(port):
    return send_request('localhost', port, {'cmd': 'increment'})

def scenario_split_brain():
    print('=== split brain scenario ===')

    nodes = []
    for i in range(5):
        node = PNCounterNode(i, port=12000 + i)
        for j in range(5):
            if j != i:
                node.add_peer(j, 'localhost', 12000 + j)
        node.start()
        nodes.append(node)

    time.sleep(1)

    print('initial state:')
    for i in range(5):
        print(f'  node {i}: {get_counter(12000 + i)}')

    print('\nincrementing on each node:')
    for i in range(5):
        increment(12000 + i)
        print(f'  node {i} after increment: {get_counter(12000 + i)}')

    print('\ncreating partition: [0,1] vs [2,3,4]')
    pm = PartitionManager(list(range(5)))
    pm.create_partition([0, 1], [2, 3, 4])

    nodes[0].peers = {1: ('localhost', 12001)}
    nodes[1].peers = {0: ('localhost', 12000)}
    nodes[2].peers = {3: ('localhost', 12003), 4: ('localhost', 12004)}
    nodes[3].peers = {2: ('localhost', 12002), 4: ('localhost', 12004)}
    nodes[4].peers = {2: ('localhost', 12002), 3: ('localhost', 12003)}

    print('\nincrementing during partition:')
    for _ in range(3):
        increment(12000)
        increment(12002)

    time.sleep(2)

    print('\nstate during partition:')
    for i in range(5):
        print(f'  node {i}: {get_counter(12000 + i)}')

    print('\nhealing partition:')
    for i in range(5):
        for j in range(5):
            if j != i:
                nodes[i].add_peer(j, 'localhost', 12000 + j)

    time.sleep(3)

    print('\nstate after healing:')
    for i in range(5):
        print(f'  node {i}: {get_counter(12000 + i)}')

    for node in nodes:
        node.stop()

def scenario_network_partition_counter():
    print('\n=== network partition with crdt counter ===')

    nodes = []
    for i in range(3):
        node = PNCounterNode(i, port=13000 + i)
        for j in range(3):
            if j != i:
                node.add_peer(j, 'localhost', 13000 + j)
        node.start()
        nodes.append(node)

    time.sleep(1)

    print('incrementing 10 times on node 0:')
    for _ in range(10):
        increment(13000)
    time.sleep(2)

    print('state after sync:')
    for i in range(3):
        print(f'  node {i}: {get_counter(13000 + i)}')

    print('\nisolating node 2:')
    nodes[2].peers = {}

    print('incrementing 5 times on node 0 and 5 times on node 2:')
    for _ in range(5):
        increment(13000)
        increment(13002)

    time.sleep(1)

    print('state during partition:')
    for i in range(3):
        print(f'  node {i}: {get_counter(13000 + i)}')

    print('\nreconnecting node 2:')
    nodes[2].add_peer(0, 'localhost', 13000)
    nodes[2].add_peer(1, 'localhost', 13001)

    time.sleep(3)

    print('final state (should converge to 20):')
    for i in range(3):
        print(f'  node {i}: {get_counter(13000 + i)}')

    for node in nodes:
        node.stop()

def scenario_quorum_loss():
    print('\n=== quorum loss scenario ===')

    nodes = []
    for i in range(5):
        node = PNCounterNode(i, port=14000 + i)
        for j in range(5):
            if j != i:
                node.add_peer(j, 'localhost', 14000 + j)
        node.start()
        nodes.append(node)

    time.sleep(1)

    quorum = 3
    print(f'quorum size: {quorum}')

    print('\nstopping nodes 2, 3, 4 (losing quorum):')
    for i in [2, 3, 4]:
        nodes[i].stop()
        nodes[0].peers.pop(i, None)
        nodes[1].peers.pop(i, None)

    time.sleep(2)

    print('attempting operations with minority:')
    alive = nodes[0]._get_alive_peers()
    print(f'  node 0 sees alive peers: {alive}')
    has_quorum = len(alive) + 1 >= quorum
    print(f'  has quorum: {has_quorum}')

    if has_quorum:
        result = increment(14000)
        print(f'  increment result: {result}')
    else:
        print('  refusing write: no quorum')

    print('\nstarting node 2 (regaining quorum):')
    nodes[2] = PNCounterNode(2, port=14002)
    nodes[2].add_peer(0, 'localhost', 14000)
    nodes[2].add_peer(1, 'localhost', 14001)
    nodes[2].start()
    nodes[0].add_peer(2, 'localhost', 14002)
    nodes[1].add_peer(2, 'localhost', 14002)

    time.sleep(2)

    alive = nodes[0]._get_alive_peers()
    print(f'node 0 sees alive peers: {alive}')
    has_quorum = len(alive) + 1 >= quorum
    print(f'has quorum: {has_quorum}')

    if has_quorum:
        result = increment(14000)
        print(f'increment result: {result}')

    for node in nodes:
        if node.running:
            node.stop()

def scenario_eventual_consistency():
    print('\n=== eventual consistency demonstration ===')

    nodes = []
    for i in range(3):
        node = PNCounterNode(i, port=15000 + i)
        node.start()
        nodes.append(node)

    print('nodes started without peer connections')

    print('\nindependent increments:')
    for i in range(3):
        for _ in range(10):
            increment(15000 + i)
        print(f'  node {i}: {get_counter(15000 + i)}')

    print('\nconnecting nodes:')
    for i in range(3):
        for j in range(3):
            if j != i:
                nodes[i].add_peer(j, 'localhost', 15000 + j)

    for _ in range(5):
        time.sleep(1)
        print('convergence progress:')
        for i in range(3):
            print(f'  node {i}: {get_counter(15000 + i)}')

    print('\nfinal converged state (should all be 30):')
    for i in range(3):
        print(f'  node {i}: {get_counter(15000 + i)}')

    for node in nodes:
        node.stop()

if __name__ == '__main__':
    scenario_split_brain()
    scenario_network_partition_counter()
    scenario_quorum_loss()
    scenario_eventual_consistency()

