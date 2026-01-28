import threading
import time
from router import ConsistentHash
from shard import ShardClient

class Rebalancer:
    def __init__(self, router, replication_factor=1):
        self.router = router
        self.replication_factor = replication_factor
        self.lock = threading.Lock()

    def add_shard(self, shard_id, host, port):
        with self.lock:
            new_client = ShardClient(host, port)
            old_ring = ConsistentHash()
            for node_key in self.router.clients.keys():
                old_ring.add_node(node_key)

            self.router.add_shard(shard_id, host, port)

            keys_to_move = {}
            for node_key, client in self.router.clients.items():
                if node_key == f'{shard_id}':
                    continue

                response = client.keys()
                if not response.get('ok'):
                    continue

                for key in response.get('keys', []):
                    new_node = self.router.hash_ring.get_node(key)
                    if new_node == f'{shard_id}':
                        if node_key not in keys_to_move:
                            keys_to_move[node_key] = []
                        keys_to_move[node_key].append(key)

            for source_node, keys in keys_to_move.items():
                source_client = self.router.clients[source_node]
                data_response = source_client.bulk_get(keys)

                if data_response.get('ok'):
                    data = data_response.get('data', {})
                    new_client.bulk_put(data)

                    for key in keys:
                        source_client.delete(key)

            return {'moved_keys': sum(len(v) for v in keys_to_move.values())}

    def remove_shard(self, shard_id):
        with self.lock:
            node_key = f'{shard_id}'
            if node_key not in self.router.clients:
                return {'error': 'shard not found'}

            removing_client = self.router.clients[node_key]
            export_response = removing_client.export_data()

            if not export_response.get('ok'):
                return {'error': 'failed to export data'}

            data = export_response.get('data', {})

            self.router.remove_shard(shard_id)

            moved = 0
            for key, value in data.items():
                target_node = self.router.hash_ring.get_node(key)
                if target_node and target_node in self.router.clients:
                    self.router.clients[target_node].put(key, value)
                    moved += 1

            return {'moved_keys': moved}

    def rebalance(self):
        with self.lock:
            distribution = self.router.get_distribution()
            if not distribution:
                return {'error': 'no shards'}

            total = sum(distribution.values())
            avg = total / len(distribution) if distribution else 0
            threshold = avg * 0.2

            overloaded = []
            underloaded = []

            for node_key, size in distribution.items():
                if size > avg + threshold:
                    overloaded.append((node_key, size - avg))
                elif size < avg - threshold:
                    underloaded.append((node_key, avg - size))

            moves = []
            for over_node, excess in overloaded:
                over_client = self.router.clients[over_node]
                keys_response = over_client.keys()

                if not keys_response.get('ok'):
                    continue

                keys = keys_response.get('keys', [])
                to_move = int(excess)

                for key in keys[:to_move]:
                    if not underloaded:
                        break

                    under_node, deficit = underloaded[0]
                    under_client = self.router.clients[under_node]

                    value_response = over_client.get(key)
                    if value_response.get('ok') and value_response.get('value'):
                        under_client.put(key, value_response['value'])
                        over_client.delete(key)
                        moves.append((key, over_node, under_node))

                        deficit -= 1
                        if deficit <= 0:
                            underloaded.pop(0)
                        else:
                            underloaded[0] = (under_node, deficit)

            return {'moves': len(moves), 'distribution': self.router.get_distribution()}

class ShardSplitter:
    def __init__(self, router):
        self.router = router

    def split_shard(self, shard_id, new_shard_id, new_host, new_port, split_point=None):
        node_key = f'{shard_id}'
        if node_key not in self.router.clients:
            return {'error': 'shard not found'}

        source_client = self.router.clients[node_key]
        keys_response = source_client.keys()

        if not keys_response.get('ok'):
            return {'error': 'failed to get keys'}

        keys = sorted(keys_response.get('keys', []))

        if split_point is None:
            split_idx = len(keys) // 2
            split_point = keys[split_idx] if split_idx < len(keys) else None

        if split_point is None:
            return {'error': 'cannot determine split point'}

        keys_to_move = [k for k in keys if k >= split_point]

        self.router.add_shard(new_shard_id, new_host, new_port)
        new_client = self.router.clients[f'{new_shard_id}']

        moved = 0
        for key in keys_to_move:
            val_response = source_client.get(key)
            if val_response.get('ok') and val_response.get('value') is not None:
                new_client.put(key, val_response['value'])
                source_client.delete(key)
                moved += 1

        return {'split_point': split_point, 'moved_keys': moved}

class ShardMerger:
    def __init__(self, router):
        self.router = router

    def merge_shards(self, shard_id_a, shard_id_b, target_shard_id=None):
        node_a = f'{shard_id_a}'
        node_b = f'{shard_id_b}'

        if node_a not in self.router.clients or node_b not in self.router.clients:
            return {'error': 'shard not found'}

        target_id = target_shard_id or shard_id_a
        target_node = f'{target_id}'
        source_id = shard_id_b if target_id == shard_id_a else shard_id_a
        source_node = f'{source_id}'

        source_client = self.router.clients[source_node]
        target_client = self.router.clients[target_node]

        export_response = source_client.export_data()
        if not export_response.get('ok'):
            return {'error': 'failed to export data'}

        data = export_response.get('data', {})
        target_client.bulk_put(data)

        self.router.remove_shard(source_id)

        return {'merged_keys': len(data), 'target_shard': target_id}

