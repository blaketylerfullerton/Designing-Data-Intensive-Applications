import hashlib
import bisect
from shard import ShardClient

class ConsistentHash:
    def __init__(self, nodes=None, replicas=150):
        self.replicas = replicas
        self.ring = []
        self.node_map = {}

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.replicas):
            virtual_key = f'{node}:{i}'
            hash_val = self._hash(virtual_key)
            bisect.insort(self.ring, hash_val)
            self.node_map[hash_val] = node

    def remove_node(self, node):
        for i in range(self.replicas):
            virtual_key = f'{node}:{i}'
            hash_val = self._hash(virtual_key)
            if hash_val in self.node_map:
                self.ring.remove(hash_val)
                del self.node_map[hash_val]

    def get_node(self, key):
        if not self.ring:
            return None

        hash_val = self._hash(key)
        idx = bisect.bisect_right(self.ring, hash_val)

        if idx == len(self.ring):
            idx = 0

        return self.node_map[self.ring[idx]]

    def get_nodes(self, key, count=1):
        if not self.ring:
            return []

        hash_val = self._hash(key)
        idx = bisect.bisect_right(self.ring, hash_val)

        nodes = []
        seen = set()

        for i in range(len(self.ring)):
            ring_idx = (idx + i) % len(self.ring)
            node = self.node_map[self.ring[ring_idx]]

            if node not in seen:
                seen.add(node)
                nodes.append(node)

                if len(nodes) >= count:
                    break

        return nodes

class ShardRouter:
    def __init__(self, shard_addrs):
        self.shard_addrs = shard_addrs
        self.hash_ring = ConsistentHash()
        self.clients = {}

        for shard_id, (host, port) in shard_addrs.items():
            node_key = f'{shard_id}'
            self.hash_ring.add_node(node_key)
            self.clients[node_key] = ShardClient(host, port)

    def _get_shard(self, key):
        node_key = self.hash_ring.get_node(key)
        return self.clients.get(node_key)

    def put(self, key, value):
        client = self._get_shard(key)
        if client:
            return client.put(key, value)
        return {'ok': False, 'error': 'no shard available'}

    def get(self, key):
        client = self._get_shard(key)
        if client:
            return client.get(key)
        return {'ok': False, 'error': 'no shard available'}

    def delete(self, key):
        client = self._get_shard(key)
        if client:
            return client.delete(key)
        return {'ok': False, 'error': 'no shard available'}

    def multi_get(self, keys):
        key_groups = {}
        for key in keys:
            node_key = self.hash_ring.get_node(key)
            if node_key not in key_groups:
                key_groups[node_key] = []
            key_groups[node_key].append(key)

        results = {}
        for node_key, group_keys in key_groups.items():
            client = self.clients.get(node_key)
            if client:
                response = client.bulk_get(group_keys)
                if response.get('ok'):
                    results.update(response.get('data', {}))

        return results

    def multi_put(self, items):
        key_groups = {}
        for key, value in items.items():
            node_key = self.hash_ring.get_node(key)
            if node_key not in key_groups:
                key_groups[node_key] = {}
            key_groups[node_key][key] = value

        total = 0
        for node_key, group_items in key_groups.items():
            client = self.clients.get(node_key)
            if client:
                response = client.bulk_put(group_items)
                if response.get('ok'):
                    total += response.get('count', 0)

        return {'ok': True, 'count': total}

    def get_distribution(self):
        dist = {}
        for node_key, client in self.clients.items():
            status = client.status()
            if status.get('ok'):
                dist[node_key] = status.get('size', 0)
        return dist

    def add_shard(self, shard_id, host, port):
        node_key = f'{shard_id}'
        self.shard_addrs[shard_id] = (host, port)
        self.hash_ring.add_node(node_key)
        self.clients[node_key] = ShardClient(host, port)

    def remove_shard(self, shard_id):
        node_key = f'{shard_id}'
        self.hash_ring.remove_node(node_key)
        self.clients.pop(node_key, None)
        self.shard_addrs.pop(shard_id, None)

class RangeRouter:
    def __init__(self, shard_addrs, key_ranges):
        self.shard_addrs = shard_addrs
        self.key_ranges = sorted(key_ranges, key=lambda x: x[0])
        self.clients = {}

        for shard_id, (host, port) in shard_addrs.items():
            self.clients[shard_id] = ShardClient(host, port)

    def _get_shard_for_key(self, key):
        for start, end, shard_id in self.key_ranges:
            if start <= key < end:
                return self.clients.get(shard_id)
        return None

    def put(self, key, value):
        client = self._get_shard_for_key(key)
        if client:
            return client.put(key, value)
        return {'ok': False, 'error': 'no shard for key'}

    def get(self, key):
        client = self._get_shard_for_key(key)
        if client:
            return client.get(key)
        return {'ok': False, 'error': 'no shard for key'}

    def range_query(self, start_key, end_key):
        results = {}
        for range_start, range_end, shard_id in self.key_ranges:
            if range_end <= start_key or range_start >= end_key:
                continue
            client = self.clients.get(shard_id)
            if client:
                response = client.keys()
                if response.get('ok'):
                    for k in response.get('keys', []):
                        if start_key <= k < end_key:
                            val = client.get(k)
                            if val.get('ok'):
                                results[k] = val.get('value')
        return results


