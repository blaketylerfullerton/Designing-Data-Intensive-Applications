import socket
import json
import threading
import time
import random

class ClusterNode:
    def __init__(self, node_id, host, port):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.last_heartbeat = 0
        self.is_alive = True

class ClusterManager:
    def __init__(self, local_node, peers, heartbeat_interval=2.0, failure_timeout=10.0):
        self.local_node = local_node
        self.peers = {
            p.node_id: p for p in peers
        }
        self.heartbeat_interval = heartbeat_interval
        self.failure_timeout = failure_timeout
        self.leader_id = None
        self.running = False
        self.lock = threading.Lock()
        self.heartbeat_thread = None
        self.monitor_thread = None
        self.on_leader_change = None

    def start(self):
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.heartbeat_thread.start()
        self.monitor_thread.start()

    def stop(self):
        self.running = False

    def _heartbeat_loop(self):
        while self.running:
            self._send_heartbeats()
            time.sleep(self.heartbeat_interval)

    def _send_heartbeats(self):
        for node_id, peer in self.peers.items():
            self._send_heartbeat(peer)

    def _send_heartbeat(self, peer):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((peer.host, peer.port))
            request = {
                'cmd': 'heartbeat',
                'from': self.local_node.node_id,
                'leader': self.leader_id,
                'timestamp': time.time()
            }
            sock.sendall(json.dumps(request).encode())
            response = json.loads(sock.recv(4096).decode())
            sock.close()

            if response.get('ok'):
                with self.lock:
                    peer.last_heartbeat = time.time()
                    peer.is_alive = True
        except:
            pass

    def _monitor_loop(self):
        while self.running:
            self._check_peers()
            self._check_leader()
            time.sleep(1.0)

    def _check_peers(self):
        now = time.time()
        with self.lock:
            for node_id, peer in self.peers.items():
                if now - peer.last_heartbeat > self.failure_timeout:
                    if peer.is_alive:
                        peer.is_alive = False

    def _check_leader(self):
        with self.lock:
            if self.leader_id is None:
                self._elect_leader()
            elif self.leader_id != self.local_node.node_id:
                if self.leader_id in self.peers and not self.peers[self.leader_id].is_alive:
                    self._elect_leader()

    def _elect_leader(self):
        candidates = [self.local_node.node_id]
        for node_id, peer in self.peers.items():
            if peer.is_alive:
                candidates.append(node_id)

        if candidates:
            new_leader = min(candidates)
            if new_leader != self.leader_id:
                self.leader_id = new_leader
                if self.on_leader_change:
                    self.on_leader_change(new_leader)

    def get_leader(self):
        with self.lock:
            return self.leader_id

    def is_leader(self):
        with self.lock:
            return self.leader_id == self.local_node.node_id

    def get_alive_peers(self):
        with self.lock:
            return [p for p in self.peers.values() if p.is_alive]

class FailoverManager:
    def __init__(self, cluster_manager, local_storage_node, replication_manager=None):
        self.cluster = cluster_manager
        self.storage = local_storage_node
        self.replication = replication_manager
        self.cluster.on_leader_change = self._handle_leader_change

    def _handle_leader_change(self, new_leader_id):
        if new_leader_id == self.storage.node_id:
            self.storage.become_leader()
            if self.replication:
                self.replication.start()
        else:
            self.storage.become_follower(new_leader_id)
            if self.replication:
                self.replication.stop()

    def force_failover(self):
        with self.cluster.lock:
            if self.cluster.leader_id in self.cluster.peers:
                self.cluster.peers[self.cluster.leader_id].is_alive = False
            self.cluster._elect_leader()

class ReplicaSet:
    def __init__(self, nodes):
        self.nodes = {n.node_id: n for n in nodes}
        self.leader_id = None

    def add_node(self, node):
        self.nodes[node.node_id] = node

    def remove_node(self, node_id):
        self.nodes.pop(node_id, None)
        if self.leader_id == node_id:
            self.leader_id = None

    def set_leader(self, node_id):
        if node_id in self.nodes:
            self.leader_id = node_id

    def get_leader(self):
        if self.leader_id and self.leader_id in self.nodes:
            return self.nodes[self.leader_id]
        return None

    def get_followers(self):
        return [n for nid, n in self.nodes.items() if nid != self.leader_id]

if __name__ == '__main__':
    from node import StorageNode

    nodes = []
    for i in range(3):
        node = StorageNode(i, port=9000 + i)
        node.start()
        nodes.append(node)

    peers = [
        ClusterNode(i, 'localhost', 9000 + i)
        for i in range(3)
    ]

    managers = []
    for i, node in enumerate(nodes):
        other_peers = [p for p in peers if p.node_id != i]
        local = ClusterNode(i, 'localhost', 9000 + i)
        manager = ClusterManager(local, other_peers)
        manager.start()
        managers.append(manager)

    time.sleep(5)

    for m in managers:
        print(f'node {m.local_node.node_id}: leader={m.get_leader()}, is_leader={m.is_leader()}')

