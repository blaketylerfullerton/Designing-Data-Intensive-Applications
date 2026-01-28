import socket
import json
import random
import time

class StoreClient:
    def __init__(self, nodes, timeout=5.0):
        self.nodes = nodes
        self.timeout = timeout
        self.leader_addr = None

    def _send(self, addr, request):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
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
            if response.get('ok') and response.get('role') == 'LEADER':
                self.leader_addr = addr
                return addr
        return None

    def _get_leader(self):
        if self.leader_addr:
            response = self._send(self.leader_addr, {'cmd': 'status'})
            if response.get('ok') and response.get('role') == 'LEADER':
                return self.leader_addr
            self.leader_addr = None
        return self._find_leader()

    def put(self, key, value):
        leader = self._get_leader()
        if not leader:
            return {'ok': False, 'error': 'no leader found'}
        response = self._send(leader, {'cmd': 'put', 'key': key, 'value': value})
        if not response.get('ok') and response.get('error') == 'not leader':
            self.leader_addr = None
            return self.put(key, value)
        return response

    def delete(self, key):
        leader = self._get_leader()
        if not leader:
            return {'ok': False, 'error': 'no leader found'}
        response = self._send(leader, {'cmd': 'delete', 'key': key})
        if not response.get('ok') and response.get('error') == 'not leader':
            self.leader_addr = None
            return self.delete(key)
        return response

    def get(self, key, read_from_leader=False):
        if read_from_leader:
            leader = self._get_leader()
            if leader:
                return self._send(leader, {'cmd': 'get', 'key': key})
        addr = random.choice(self.nodes)
        return self._send(addr, {'cmd': 'get', 'key': key})

    def get_consistent(self, key):
        return self.get(key, read_from_leader=True)

class ReadYourWritesClient:
    def __init__(self, base_client):
        self.client = base_client
        self.last_write_seq = -1

    def put(self, key, value):
        response = self.client.put(key, value)
        if response.get('ok'):
            self.last_write_seq = response.get('seq', self.last_write_seq)
        return response

    def get(self, key):
        return self.client.get_consistent(key)

class MonotonicReadsClient:
    def __init__(self, base_client):
        self.client = base_client
        self.last_read_seq = {}

    def get(self, key):
        response = self.client.get_consistent(key)
        return response

    def put(self, key, value):
        return self.client.put(key, value)

if __name__ == '__main__':
    nodes = [
        ('localhost', 9000),
        ('localhost', 9001),
        ('localhost', 9002)
    ]

    client = StoreClient(nodes)

    print('writing to leader:')
    for i in range(10):
        response = client.put(f'key_{i}', f'value_{i}')
        print(f'  put key_{i}: {response}')

    print('\nreading from random replica:')
    for i in range(10):
        response = client.get(f'key_{i}')
        print(f'  get key_{i}: {response}')

    print('\nconsistent read from leader:')
    for i in range(5):
        response = client.get_consistent(f'key_{i}')
        print(f'  get_consistent key_{i}: {response}')

    print('\nread-your-writes session:')
    ryw_client = ReadYourWritesClient(client)
    ryw_client.put('session_key', 'session_value')
    response = ryw_client.get('session_key')
    print(f'  read after write: {response}')


