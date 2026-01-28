import json
import threading
from pathlib import Path
from collections import defaultdict

class GraphStore:
    def __init__(self, data_file='graph_data.json'):
        self.data_file = Path(data_file)
        self.lock = threading.Lock()
        self.nodes = {}
        self.edges = defaultdict(list)
        self.reverse_edges = defaultdict(list)
        self._load()

    def _load(self):
        if self.data_file.exists():
            with open(self.data_file) as f:
                data = json.load(f)
                self.nodes = data.get('nodes', {})
                for from_id, rels in data.get('edges', {}).items():
                    self.edges[from_id] = rels
                    for rel in rels:
                        self.reverse_edges[rel['to']].append({
                            'from': from_id,
                            'type': rel['type']
                        })

    def _save(self):
        data = {
            'nodes': self.nodes,
            'edges': dict(self.edges)
        }
        with open(self.data_file, 'w') as f:
            json.dump(data, f)

    def create_user(self, user_id, data):
        with self.lock:
            node = dict(data)
            node['id'] = user_id
            self.nodes[user_id] = node
            self._save()

    def get_user(self, user_id):
        node = self.nodes.get(user_id)
        if node:
            return dict(node)
        return None

    def update_user(self, user_id, data):
        with self.lock:
            if user_id in self.nodes:
                self.nodes[user_id].update(data)
                self._save()

    def delete_user(self, user_id):
        with self.lock:
            if user_id in self.nodes:
                del self.nodes[user_id]

            if user_id in self.edges:
                for rel in self.edges[user_id]:
                    to_id = rel['to']
                    self.reverse_edges[to_id] = [
                        r for r in self.reverse_edges[to_id]
                        if r['from'] != user_id
                    ]
                del self.edges[user_id]

            if user_id in self.reverse_edges:
                for rel in self.reverse_edges[user_id]:
                    from_id = rel['from']
                    self.edges[from_id] = [
                        r for r in self.edges[from_id]
                        if r['to'] != user_id
                    ]
                del self.reverse_edges[user_id]

            self._save()

    def add_relationship(self, from_id, to_id, rel_type):
        with self.lock:
            for rel in self.edges[from_id]:
                if rel['to'] == to_id and rel['type'] == rel_type:
                    return

            self.edges[from_id].append({'to': to_id, 'type': rel_type})
            self.reverse_edges[to_id].append({'from': from_id, 'type': rel_type})
            self._save()

    def get_relationships(self, user_id, rel_type=None):
        rels = self.edges.get(user_id, [])
        if rel_type:
            rels = [r for r in rels if r['type'] == rel_type]
        return [{'to': r['to'], 'type': r['type']} for r in rels]

    def query_users(self, filters):
        results = []
        for user_id, node in self.nodes.items():
            match = True
            for key, value in filters.items():
                if key not in node:
                    match = False
                    break
                node_value = node[key]
                if isinstance(node_value, str):
                    if value.lower() not in node_value.lower():
                        match = False
                        break
                else:
                    if str(node_value) != str(value):
                        match = False
                        break
            if match:
                results.append(dict(node))
        return results

    def traverse(self, start_id, rel_type, depth=1):
        visited = set()
        current_level = {start_id}
        results = []

        for _ in range(depth):
            next_level = set()
            for node_id in current_level:
                if node_id in visited:
                    continue
                visited.add(node_id)

                for rel in self.edges.get(node_id, []):
                    if rel_type is None or rel['type'] == rel_type:
                        to_id = rel['to']
                        if to_id not in visited:
                            next_level.add(to_id)
                            node = self.get_user(to_id)
                            if node:
                                results.append(node)
            current_level = next_level

        return results

    def shortest_path(self, from_id, to_id, rel_type=None):
        if from_id == to_id:
            return [from_id]

        visited = {from_id}
        queue = [(from_id, [from_id])]

        while queue:
            current, path = queue.pop(0)

            for rel in self.edges.get(current, []):
                if rel_type and rel['type'] != rel_type:
                    continue

                next_id = rel['to']
                if next_id == to_id:
                    return path + [next_id]

                if next_id not in visited:
                    visited.add(next_id)
                    queue.append((next_id, path + [next_id]))

        return None

