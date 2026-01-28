import socket
import threading
import time
import random
from collections import defaultdict

class NetworkSimulator:
    def __init__(self):
        self.partitions = set()
        self.latency = {}
        self.packet_loss = {}
        self.lock = threading.Lock()

    def add_partition(self, node_a, node_b):
        with self.lock:
            self.partitions.add((min(node_a, node_b), max(node_a, node_b)))

    def remove_partition(self, node_a, node_b):
        with self.lock:
            self.partitions.discard((min(node_a, node_b), max(node_a, node_b)))

    def clear_partitions(self):
        with self.lock:
            self.partitions.clear()

    def is_partitioned(self, node_a, node_b):
        with self.lock:
            return (min(node_a, node_b), max(node_a, node_b)) in self.partitions

    def set_latency(self, node_a, node_b, latency_ms):
        with self.lock:
            self.latency[(min(node_a, node_b), max(node_a, node_b))] = latency_ms

    def get_latency(self, node_a, node_b):
        with self.lock:
            return self.latency.get((min(node_a, node_b), max(node_a, node_b)), 0)

    def set_packet_loss(self, node_a, node_b, loss_rate):
        with self.lock:
            self.packet_loss[(min(node_a, node_b), max(node_a, node_b))] = loss_rate

    def should_drop(self, node_a, node_b):
        with self.lock:
            rate = self.packet_loss.get((min(node_a, node_b), max(node_a, node_b)), 0)
            return random.random() < rate

class ProxyNode:
    def __init__(self, node_id, real_host, real_port, proxy_port, network_sim):
        self.node_id = node_id
        self.real_host = real_host
        self.real_port = real_port
        self.proxy_port = proxy_port
        self.network = network_sim
        self.running = False
        self.server_socket = None

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('localhost', self.proxy_port))
        self.server_socket.listen(10)
        threading.Thread(target=self._serve, daemon=True).start()

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

            import json
            try:
                request = json.loads(data.decode())
                from_node = request.get('_from_node', -1)
            except:
                from_node = -1

            if from_node >= 0 and self.network.is_partitioned(from_node, self.node_id):
                conn.close()
                return

            if from_node >= 0 and self.network.should_drop(from_node, self.node_id):
                conn.close()
                return

            latency = self.network.get_latency(from_node, self.node_id) if from_node >= 0 else 0
            if latency > 0:
                time.sleep(latency / 1000.0)

            real_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            real_sock.settimeout(5.0)
            real_sock.connect((self.real_host, self.real_port))
            real_sock.sendall(data)
            response = real_sock.recv(65536)
            real_sock.close()

            if latency > 0:
                time.sleep(latency / 1000.0)

            conn.sendall(response)
        except:
            pass
        finally:
            conn.close()

class PartitionManager:
    def __init__(self, nodes):
        self.nodes = nodes
        self.network = NetworkSimulator()
        self.proxies = {}

    def create_partition(self, group_a, group_b):
        for a in group_a:
            for b in group_b:
                self.network.add_partition(a, b)

    def heal_partition(self, group_a, group_b):
        for a in group_a:
            for b in group_b:
                self.network.remove_partition(a, b)

    def isolate_node(self, node_id):
        for other_id in self.nodes:
            if other_id != node_id:
                self.network.add_partition(node_id, other_id)

    def reconnect_node(self, node_id):
        for other_id in self.nodes:
            if other_id != node_id:
                self.network.remove_partition(node_id, other_id)

    def get_partition_state(self):
        return list(self.network.partitions)

class SplitBrainDetector:
    def __init__(self, nodes, quorum_size):
        self.nodes = nodes
        self.quorum_size = quorum_size

    def check_quorum(self, visible_nodes):
        return len(visible_nodes) >= self.quorum_size

    def detect_split_brain(self, partition_groups):
        leaders = 0
        for group in partition_groups:
            if len(group) >= self.quorum_size:
                leaders += 1
        return leaders > 1

