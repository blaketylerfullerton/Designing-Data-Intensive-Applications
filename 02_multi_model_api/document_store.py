import json
import os
import threading
from pathlib import Path

class DocumentStore:
    def __init__(self, data_dir='document_data'):
        self.data_dir = Path(data_dir)
        self.users_dir = self.data_dir / 'users'
        self.rels_dir = self.data_dir / 'relationships'
        self.users_dir.mkdir(parents=True, exist_ok=True)
        self.rels_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def _user_path(self, user_id):
        return self.users_dir / f'{user_id}.json'

    def _rels_path(self, user_id):
        return self.rels_dir / f'{user_id}.json'

    def create_user(self, user_id, data):
        with self.lock:
            doc = dict(data)
            doc['_id'] = user_id
            with open(self._user_path(user_id), 'w') as f:
                json.dump(doc, f)

    def get_user(self, user_id):
        path = self._user_path(user_id)
        if not path.exists():
            return None
        with open(path) as f:
            doc = json.load(f)
        result = dict(doc)
        if '_id' in result:
            result['id'] = result.pop('_id')
        return result

    def update_user(self, user_id, data):
        existing = self.get_user(user_id)
        if existing:
            existing.update(data)
            self.create_user(user_id, existing)

    def delete_user(self, user_id):
        with self.lock:
            path = self._user_path(user_id)
            if path.exists():
                path.unlink()
            rels_path = self._rels_path(user_id)
            if rels_path.exists():
                rels_path.unlink()

    def add_relationship(self, from_id, to_id, rel_type):
        with self.lock:
            rels_path = self._rels_path(from_id)
            if rels_path.exists():
                with open(rels_path) as f:
                    rels = json.load(f)
            else:
                rels = []

            for r in rels:
                if r['to'] == to_id and r['type'] == rel_type:
                    return

            rels.append({'to': to_id, 'type': rel_type})

            with open(rels_path, 'w') as f:
                json.dump(rels, f)

    def get_relationships(self, user_id, rel_type=None):
        rels_path = self._rels_path(user_id)
        if not rels_path.exists():
            return []

        with open(rels_path) as f:
            rels = json.load(f)

        if rel_type:
            rels = [r for r in rels if r['type'] == rel_type]

        return rels

    def query_users(self, filters):
        results = []
        for path in self.users_dir.glob('*.json'):
            with open(path) as f:
                doc = json.load(f)

            match = True
            for key, value in filters.items():
                if key not in doc:
                    match = False
                    break
                doc_value = doc[key]
                if isinstance(doc_value, str):
                    if value.lower() not in doc_value.lower():
                        match = False
                        break
                else:
                    if str(doc_value) != str(value):
                        match = False
                        break

            if match:
                result = dict(doc)
                if '_id' in result:
                    result['id'] = result.pop('_id')
                results.append(result)

        return results


