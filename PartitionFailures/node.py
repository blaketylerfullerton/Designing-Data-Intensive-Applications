import socket
import json
import threading
import time
from collections import defaultdict

class CounterNode:
    def __init__(self, node_id, host='localhost', port=12000):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.counter = 0
        self.vector_clock = defaultdict(int)
        self.peers = {}
        self.lock = threading.Lock()
        self.running = False
        self.server_socket = None
        self.heartbeat_thread = None
        self.last_heartbeat = {}
        self.is_leader = False
        self.leader_id = None
        self.term = 0

    def add_peer(self, node_id, host, port):
        self.peers[node_id] = (host, port)
        self.last_heartbeat[node_id] = 0

    def increment(self):
        with self.lock:
            self.counter += 1
            self.vector_clock[self.node_id] += 1
            return self.counter, dict(self.vector_clock)

    def decrement(self):
        with self.lock:
            self.counter -= 1
            self.vector_clock[self.node_id] += 1
            return self.counter, dict(self.vector_clock)

    def get(self):
        with self.lock:
            return self.counter, dict(self.vector_clock)

    def merge(self, remote_counter, remote_clock, remote_node):
        with self.lock:
            for node, ts in remote_clock.items():
                self.vector_clock[node] = max(self.vector_clock[node], ts)
            return self.counter, dict(self.vector_clock)

    def sync_with_peer(self, peer_id):
        if peer_id not in self.peers:
            return False

        host, port = self.peers[peer_id]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))

            counter, clock = self.get()
            request = {
                'cmd': 'sync',
                'node_id': self.node_id,
                'counter': counter,
                'clock': clock
            }
            sock.sendall(json.dumps(request).encode())
            response = json.loads(sock.recv(65536).decode())
            sock.close()

            if response.get('ok'):
                self.merge(response['counter'], response['clock'], peer_id)
                return True
        except:
            pass
        return False

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

        threading.Thread(target=self._serve, daemon=True).start()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def stop(self):
        self.running = False
        if self.server_socket:
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
            counter, clock = self.get()
            return {'ok': True, 'counter': counter, 'clock': clock}

        if cmd == 'increment':
            counter, clock = self.increment()
            return {'ok': True, 'counter': counter, 'clock': clock}

        if cmd == 'decrement':
            counter, clock = self.decrement()
            return {'ok': True, 'counter': counter, 'clock': clock}

        if cmd == 'sync':
            remote_counter = request['counter']
            remote_clock = request['clock']
            remote_node = request['node_id']
            self.merge(remote_counter, remote_clock, remote_node)
            counter, clock = self.get()
            return {'ok': True, 'counter': counter, 'clock': clock}

        if cmd == 'heartbeat':
            sender = request['node_id']
            with self.lock:
                self.last_heartbeat[sender] = time.time()
            return {'ok': True, 'node_id': self.node_id}

        if cmd == 'status':
            return {
                'ok': True,
                'node_id': self.node_id,
                'counter': self.counter,
                'clock': dict(self.vector_clock),
                'is_leader': self.is_leader,
                'leader_id': self.leader_id,
                'peers_alive': self._get_alive_peers()
            }

        return {'ok': False, 'error': 'unknown command'}

    def _heartbeat_loop(self):
        while self.running:
            for peer_id, (host, port) in self.peers.items():
                self._send_heartbeat(peer_id, host, port)
            time.sleep(1.0)

    def _send_heartbeat(self, peer_id, host, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((host, port))
            request = {'cmd': 'heartbeat', 'node_id': self.node_id}
            sock.sendall(json.dumps(request).encode())
            response = json.loads(sock.recv(4096).decode())
            sock.close()
            if response.get('ok'):
                with self.lock:
                    self.last_heartbeat[peer_id] = time.time()
        except:
            pass

    def _get_alive_peers(self):
        now = time.time()
        alive = []
        for peer_id in self.peers:
            if now - self.last_heartbeat.get(peer_id, 0) < 5.0:
                alive.append(peer_id)
        return alive

class GCounterNode(CounterNode):
    def __init__(self, node_id, host='localhost', port=12000):
        super().__init__(node_id, host, port)
        self.increments = defaultdict(int)

    def increment(self):
        with self.lock:
            self.increments[self.node_id] += 1
            self.counter = sum(self.increments.values())
            self.vector_clock[self.node_id] += 1
            return self.counter, dict(self.vector_clock)

    def merge(self, remote_increments, remote_clock, remote_node):
        with self.lock:
            for node, count in remote_increments.items():
                self.increments[node] = max(self.increments[node], count)
            self.counter = sum(self.increments.values())
            for node, ts in remote_clock.items():
                self.vector_clock[node] = max(self.vector_clock[node], ts)
            return self.counter, dict(self.vector_clock)

    def get(self):
        with self.lock:
            return self.counter, dict(self.increments), dict(self.vector_clock)

    def _process(self, request):
        cmd = request.get('cmd')

        if cmd == 'get':
            counter, increments, clock = self.get()
            return {'ok': True, 'counter': counter, 'increments': increments, 'clock': clock}

        if cmd == 'increment':
            counter, clock = self.increment()
            return {'ok': True, 'counter': counter, 'clock': clock}

        if cmd == 'sync':
            remote_increments = request.get('increments', {})
            remote_clock = request['clock']
            remote_node = request['node_id']
            self.merge(remote_increments, remote_clock, remote_node)
            counter, increments, clock = self.get()
            return {'ok': True, 'counter': counter, 'increments': increments, 'clock': clock}

        return super()._process(request)

class PNCounterNode(GCounterNode):
    def __init__(self, node_id, host='localhost', port=12000):
        super().__init__(node_id, host, port)
        self.decrements = defaultdict(int)

    def decrement(self):
        with self.lock:
            self.decrements[self.node_id] += 1
            self.counter = sum(self.increments.values()) - sum(self.decrements.values())
            self.vector_clock[self.node_id] += 1
            return self.counter, dict(self.vector_clock)

    def merge(self, remote_increments, remote_decrements, remote_clock, remote_node):
        with self.lock:
            for node, count in remote_increments.items():
                self.increments[node] = max(self.increments[node], count)
            for node, count in remote_decrements.items():
                self.decrements[node] = max(self.decrements[node], count)
            self.counter = sum(self.increments.values()) - sum(self.decrements.values())
            for node, ts in remote_clock.items():
                self.vector_clock[node] = max(self.vector_clock[node], ts)
            return self.counter, dict(self.vector_clock)

    def get(self):
        with self.lock:
            return self.counter, dict(self.increments), dict(self.decrements), dict(self.vector_clock)

if __name__ == '__main__':
    import sys
    node_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    port = 12000 + node_id
    node = PNCounterNode(node_id, port=port)

    for i in range(3):
        if i != node_id:
            node.add_peer(i, 'localhost', 12000 + i)

    node.start()
    print(f'node {node_id} started on port {port}')

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()


