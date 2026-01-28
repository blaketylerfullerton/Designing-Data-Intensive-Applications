import socket
import json
import threading
import time
from raft_node import RaftNode
from state_machine import KeyValueStateMachine

class ConsensusCluster:
    def __init__(self, node_configs):
        self.nodes = {}
        self.state_machines = {}

        for node_id, config in node_configs.items():
            peers = {
                nid: (cfg['host'], cfg['port'])
                for nid, cfg in node_configs.items()
                if nid != node_id
            }

            node = RaftNode(
                node_id,
                peers,
                host=config['host'],
                port=config['port']
            )

            sm = KeyValueStateMachine()
            node.apply_callback = lambda entry, sm=sm: sm.apply(entry)

            self.nodes[node_id] = node
            self.state_machines[node_id] = sm

    def start(self):
        for node in self.nodes.values():
            node.start()

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def get_leader(self):
        for node_id, node in self.nodes.items():
            status = self._get_status(node)
            if status and status.get('state') == 'LEADER':
                return node_id
        return None

    def _get_status(self, node):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((node.host, node.port))
            sock.sendall(json.dumps({'cmd': 'status'}).encode())
            response = json.loads(sock.recv(65536).decode())
            sock.close()
            return response
        except:
            return None

class ConsensusClient:
    def __init__(self, nodes):
        self.nodes = nodes
        self.leader_addr = None

    def _send(self, addr, request):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(addr)
            sock.sendall(json.dumps(request).encode())
            response = json.loads(sock.recv(65536).decode())
            sock.close()
            return response
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def _find_leader(self):
        for addr in self.nodes:
            response = self._send(addr, {'cmd': 'status'})
            if response.get('ok') and response.get('state') == 'LEADER':
                self.leader_addr = addr
                return addr
        return None

    def _get_leader(self):
        if self.leader_addr:
            response = self._send(self.leader_addr, {'cmd': 'status'})
            if response.get('ok') and response.get('state') == 'LEADER':
                return self.leader_addr
            self.leader_addr = None
        return self._find_leader()

    def set(self, key, value):
        leader = self._get_leader()
        if not leader:
            return {'ok': False, 'error': 'no leader'}

        response = self._send(leader, {
            'cmd': 'client_request',
            'command': {'op': 'set', 'key': key, 'value': value}
        })

        if response.get('error') == 'not leader':
            self.leader_addr = None
            return self.set(key, value)

        return response

    def get(self, key):
        leader = self._get_leader()
        if not leader:
            return {'ok': False, 'error': 'no leader'}

        return self._send(leader, {
            'cmd': 'client_request',
            'command': {'op': 'get', 'key': key}
        })

    def delete(self, key):
        leader = self._get_leader()
        if not leader:
            return {'ok': False, 'error': 'no leader'}

        return self._send(leader, {
            'cmd': 'client_request',
            'command': {'op': 'delete', 'key': key}
        })

    def cas(self, key, expected, value):
        leader = self._get_leader()
        if not leader:
            return {'ok': False, 'error': 'no leader'}

        return self._send(leader, {
            'cmd': 'client_request',
            'command': {'op': 'cas', 'key': key, 'expected': expected, 'value': value}
        })

    def status(self):
        results = {}
        for addr in self.nodes:
            response = self._send(addr, {'cmd': 'status'})
            results[f'{addr[0]}:{addr[1]}'] = response
        return results

if __name__ == '__main__':
    configs = {
        0: {'host': 'localhost', 'port': 16000},
        1: {'host': 'localhost', 'port': 16001},
        2: {'host': 'localhost', 'port': 16002}
    }

    cluster = ConsensusCluster(configs)
    cluster.start()

    print('waiting for leader election...')
    time.sleep(5)

    client = ConsensusClient([
        ('localhost', 16000),
        ('localhost', 16001),
        ('localhost', 16002)
    ])

    print('\ncluster status:')
    status = client.status()
    for addr, s in status.items():
        print(f'  {addr}: {s}')

    print('\nwriting data:')
    for i in range(5):
        result = client.set(f'key_{i}', f'value_{i}')
        print(f'  set key_{i}: {result}')

    time.sleep(2)

    print('\nreading data:')
    for i in range(5):
        result = client.get(f'key_{i}')
        print(f'  get key_{i}: {result}')

    print('\ncas operation:')
    result = client.cas('key_0', 'value_0', 'new_value_0')
    print(f'  cas key_0 (expected value_0): {result}')

    result = client.cas('key_0', 'value_0', 'another_value')
    print(f'  cas key_0 (expected value_0, wrong): {result}')

    cluster.stop()


