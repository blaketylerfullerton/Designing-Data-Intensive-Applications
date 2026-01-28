import math
import time
import threading
from collections import deque

class PhiAccrualDetector:
    def __init__(self, threshold=8.0, window_size=100, min_std_dev=500):
        self.threshold = threshold
        self.window_size = window_size
        self.min_std_dev = min_std_dev
        self.heartbeat_history = {}
        self.last_heartbeat = {}
        self.lock = threading.Lock()

    def heartbeat(self, node_id):
        with self.lock:
            now = time.time() * 1000

            if node_id not in self.heartbeat_history:
                self.heartbeat_history[node_id] = deque(maxlen=self.window_size)
            else:
                if self.last_heartbeat.get(node_id):
                    interval = now - self.last_heartbeat[node_id]
                    self.heartbeat_history[node_id].append(interval)

            self.last_heartbeat[node_id] = now

    def phi(self, node_id):
        with self.lock:
            if node_id not in self.last_heartbeat:
                return float('inf')

            history = self.heartbeat_history.get(node_id)
            if not history or len(history) < 2:
                return 0.0

            now = time.time() * 1000
            time_since_last = now - self.last_heartbeat[node_id]

            mean = sum(history) / len(history)
            variance = sum((x - mean) ** 2 for x in history) / len(history)
            std_dev = max(math.sqrt(variance), self.min_std_dev)

            y = (time_since_last - mean) / std_dev
            e = math.exp(-y * (math.pi / math.sqrt(6)))
            p = 1.0 / (1.0 + e)

            if p == 0:
                return float('inf')

            return -math.log10(p)

    def is_alive(self, node_id):
        return self.phi(node_id) < self.threshold

    def get_all_phi(self):
        with self.lock:
            return {
                node_id: self.phi(node_id)
                for node_id in self.last_heartbeat
            }

class AdaptiveDetector:
    def __init__(self, base_timeout=5.0, alpha=0.1):
        self.base_timeout = base_timeout
        self.alpha = alpha
        self.estimated_rtt = {}
        self.rtt_variance = {}
        self.last_heartbeat = {}
        self.lock = threading.Lock()

    def heartbeat(self, node_id, rtt=None):
        with self.lock:
            now = time.time()

            if rtt is not None:
                if node_id not in self.estimated_rtt:
                    self.estimated_rtt[node_id] = rtt
                    self.rtt_variance[node_id] = rtt / 2
                else:
                    self.rtt_variance[node_id] = (1 - self.alpha) * self.rtt_variance[node_id] + \
                                                  self.alpha * abs(self.estimated_rtt[node_id] - rtt)
                    self.estimated_rtt[node_id] = (1 - self.alpha) * self.estimated_rtt[node_id] + \
                                                   self.alpha * rtt

            self.last_heartbeat[node_id] = now

    def get_timeout(self, node_id):
        with self.lock:
            if node_id not in self.estimated_rtt:
                return self.base_timeout

            return self.estimated_rtt[node_id] + 4 * self.rtt_variance[node_id]

    def is_alive(self, node_id):
        with self.lock:
            if node_id not in self.last_heartbeat:
                return False

            timeout = self.get_timeout(node_id)
            return time.time() - self.last_heartbeat[node_id] < timeout

class GossipDetector:
    def __init__(self, local_node_id, gossip_interval=1.0, suspect_timeout=5.0, fail_timeout=15.0):
        self.local_node_id = local_node_id
        self.gossip_interval = gossip_interval
        self.suspect_timeout = suspect_timeout
        self.fail_timeout = fail_timeout
        self.heartbeat_counters = {}
        self.last_update = {}
        self.suspected = set()
        self.failed = set()
        self.lock = threading.Lock()

    def local_heartbeat(self):
        with self.lock:
            if self.local_node_id not in self.heartbeat_counters:
                self.heartbeat_counters[self.local_node_id] = 0
            self.heartbeat_counters[self.local_node_id] += 1
            self.last_update[self.local_node_id] = time.time()

    def receive_gossip(self, remote_counters):
        with self.lock:
            now = time.time()
            for node_id, counter in remote_counters.items():
                if node_id not in self.heartbeat_counters or counter > self.heartbeat_counters[node_id]:
                    self.heartbeat_counters[node_id] = counter
                    self.last_update[node_id] = now
                    self.suspected.discard(node_id)
                    self.failed.discard(node_id)

    def get_gossip_state(self):
        with self.lock:
            return dict(self.heartbeat_counters)

    def check_nodes(self):
        with self.lock:
            now = time.time()
            for node_id, last in list(self.last_update.items()):
                if node_id == self.local_node_id:
                    continue

                age = now - last

                if age > self.fail_timeout:
                    self.suspected.discard(node_id)
                    self.failed.add(node_id)
                elif age > self.suspect_timeout:
                    if node_id not in self.failed:
                        self.suspected.add(node_id)
                else:
                    self.suspected.discard(node_id)
                    self.failed.discard(node_id)

    def get_alive_nodes(self):
        with self.lock:
            return [
                node_id for node_id in self.heartbeat_counters
                if node_id not in self.suspected and node_id not in self.failed
            ]

    def get_suspected_nodes(self):
        with self.lock:
            return list(self.suspected)

    def get_failed_nodes(self):
        with self.lock:
            return list(self.failed)


