import socket
import json
import threading
import time
from router import ShardRouter
from rebalancer import Rebalancer, ShardSplitter

class ClusterCoordinator:
    def __init__(self, host='localhost', port=11000):
        self.host = host
        self.port = port
        self.shards = {}
        self.router = None
        self.rebalancer = None
        self.running = False
        self.lock = threading.Lock()
        self.membership_version = 0

    def initialize(self, initial_shards):
        with self.lock:
            self.shards = dict(initial_shards)
            self.router = ShardRouter(dict(self.shards))
            self.rebalancer = Rebalancer(self.router)
            self.membership_version += 1

    def add_shard(self, shard_id, host, port):
        with self.lock:
            if shard_id in self.shards:
                return {'ok': False, 'error': 'shard exists'}

            self.shards[shard_id] = (host, port)
            result = self.rebalancer.add_shard(shard_id, host, port)
            self.membership_version += 1
            return {'ok': True, 'result': result, 'version': self.membership_version}

    def remove_shard(self, shard_id):
        with self.lock:
            if shard_id not in self.shards:
                return {'ok': False, 'error': 'shard not found'}

            result = self.rebalancer.remove_shard(shard_id)
            del self.shards[shard_id]
            self.membership_version += 1
            return {'ok': True, 'result': result, 'version': self.membership_version}

    def get_membership(self):
        with self.lock:
            return {
                'shards': dict(self.shards),
                'version': self.membership_version
            }

    def get_distribution(self):
        if self.router:
            return self.router.get_distribution()
        return {}

    def rebalance(self):
        if self.rebalancer:
            return self.rebalancer.rebalance()
        return {'error': 'not initialized'}

    def get(self, key):
        if self.router:
            return self.router.get(key)
        return {'ok': False, 'error': 'not initialized'}

    def put(self, key, value):
        if self.router:
            return self.router.put(key, value)
        return {'ok': False, 'error': 'not initialized'}

    def delete(self, key):
        if self.router:
            return self.router.delete(key)
        return {'ok': False, 'error': 'not initialized'}

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.server_thread = threading.Thread(target=self._serve, daemon=True)
        self.server_thread.start()

    def stop(self):
        self.running = False
        if hasattr(self, 'server_socket'):
            self.server_socket.close()

    def _serve(self):
        while self.running:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle, args=(conn,), daemon=True).start()
            except:
                break

    def _handle(self, conn):
        try:
            data = conn.recv(65536)
            if not data:
                return
            request = json.loads(data.decode())
            response = self._process(request)
            conn.sendall(json.dumps(response).encode())
        finally:
            conn.close()

    def _process(self, request):
        cmd = request.get('cmd')

        if cmd == 'get':
            return self.get(request['key'])

        if cmd == 'put':
            return self.put(request['key'], request['value'])

        if cmd == 'delete':
            return self.delete(request['key'])

        if cmd == 'add_shard':
            return self.add_shard(request['shard_id'], request['host'], request['port'])

        if cmd == 'remove_shard':
            return self.remove_shard(request['shard_id'])

        if cmd == 'membership':
            return {'ok': True, **self.get_membership()}

        if cmd == 'distribution':
            return {'ok': True, 'distribution': self.get_distribution()}

        if cmd == 'rebalance':
            return {'ok': True, 'result': self.rebalance()}

        if cmd == 'multi_get':
            if self.router:
                return {'ok': True, 'data': self.router.multi_get(request['keys'])}
            return {'ok': False, 'error': 'not initialized'}

        if cmd == 'multi_put':
            if self.router:
                return self.router.multi_put(request['items'])
            return {'ok': False, 'error': 'not initialized'}

        return {'ok': False, 'error': 'unknown command'}

class CoordinatorClient:
    def __init__(self, host='localhost', port=11000, timeout=10.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.cached_membership = None
        self.cached_version = -1

    def _send(self, request):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.timeout)
            sock.connect((self.host, self.port))
            sock.sendall(json.dumps(request).encode())
            response = json.loads(sock.recv(1048576).decode())
            sock.close()
            return response
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def get(self, key):
        return self._send({'cmd': 'get', 'key': key})

    def put(self, key, value):
        return self._send({'cmd': 'put', 'key': key, 'value': value})

    def delete(self, key):
        return self._send({'cmd': 'delete', 'key': key})

    def multi_get(self, keys):
        return self._send({'cmd': 'multi_get', 'keys': keys})

    def multi_put(self, items):
        return self._send({'cmd': 'multi_put', 'items': items})

    def add_shard(self, shard_id, host, port):
        return self._send({'cmd': 'add_shard', 'shard_id': shard_id, 'host': host, 'port': port})

    def remove_shard(self, shard_id):
        return self._send({'cmd': 'remove_shard', 'shard_id': shard_id})

    def get_membership(self):
        response = self._send({'cmd': 'membership'})
        if response.get('ok'):
            self.cached_membership = response.get('shards')
            self.cached_version = response.get('version')
        return response

    def get_distribution(self):
        return self._send({'cmd': 'distribution'})

    def rebalance(self):
        return self._send({'cmd': 'rebalance'})

if __name__ == '__main__':
    from shard import ShardServer

    shards = []
    for i in range(3):
        server = ShardServer(i, port=10000 + i)
        server.start()
        shards.append(server)

    time.sleep(1)

    coord = ClusterCoordinator()
    coord.initialize({
        0: ('localhost', 10000),
        1: ('localhost', 10001),
        2: ('localhost', 10002)
    })
    coord.start()

    print('coordinator started')

    client = CoordinatorClient()

    for i in range(100):
        client.put(f'key_{i}', f'value_{i}')

    print(f'distribution: {client.get_distribution()}')

    for i in range(0, 100, 20):
        print(f'get key_{i}: {client.get(f"key_{i}")}')

    print(f'membership: {client.get_membership()}')

