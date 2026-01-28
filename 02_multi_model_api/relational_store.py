import sqlite3
import json
import threading

class RelationalStore:
    def __init__(self, db_path='relational.db'):
        self.db_path = db_path
        self.local = threading.local()
        self._init_schema()

    def _get_conn(self):
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn

    def _init_schema(self):
        conn = self._get_conn()
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                name TEXT,
                email TEXT,
                age INTEGER,
                metadata TEXT
            );
            CREATE TABLE IF NOT EXISTS relationships (
                from_id TEXT,
                to_id TEXT,
                rel_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (from_id, to_id, rel_type),
                FOREIGN KEY (from_id) REFERENCES users(id),
                FOREIGN KEY (to_id) REFERENCES users(id)
            );
            CREATE INDEX IF NOT EXISTS idx_rel_from ON relationships(from_id);
            CREATE INDEX IF NOT EXISTS idx_rel_to ON relationships(to_id);
            CREATE INDEX IF NOT EXISTS idx_rel_type ON relationships(rel_type);
        ''')
        conn.commit()

    def create_user(self, user_id, data):
        conn = self._get_conn()
        metadata = {k: v for k, v in data.items() if k not in ('id', 'name', 'email', 'age')}
        conn.execute(
            'INSERT OR REPLACE INTO users (id, name, email, age, metadata) VALUES (?, ?, ?, ?, ?)',
            (user_id, data.get('name'), data.get('email'), data.get('age'), json.dumps(metadata))
        )
        conn.commit()

    def get_user(self, user_id):
        conn = self._get_conn()
        row = conn.execute('SELECT * FROM users WHERE id = ?', (user_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        if result.get('metadata'):
            extra = json.loads(result['metadata'])
            del result['metadata']
            result.update(extra)
        return result

    def update_user(self, user_id, data):
        existing = self.get_user(user_id)
        if existing:
            existing.update(data)
            self.create_user(user_id, existing)

    def delete_user(self, user_id):
        conn = self._get_conn()
        conn.execute('DELETE FROM users WHERE id = ?', (user_id,))
        conn.execute('DELETE FROM relationships WHERE from_id = ? OR to_id = ?', (user_id, user_id))
        conn.commit()

    def add_relationship(self, from_id, to_id, rel_type):
        conn = self._get_conn()
        conn.execute(
            'INSERT OR REPLACE INTO relationships (from_id, to_id, rel_type) VALUES (?, ?, ?)',
            (from_id, to_id, rel_type)
        )
        conn.commit()

    def get_relationships(self, user_id, rel_type=None):
        conn = self._get_conn()
        if rel_type:
            rows = conn.execute(
                'SELECT to_id, rel_type FROM relationships WHERE from_id = ? AND rel_type = ?',
                (user_id, rel_type)
            ).fetchall()
        else:
            rows = conn.execute(
                'SELECT to_id, rel_type FROM relationships WHERE from_id = ?',
                (user_id,)
            ).fetchall()
        return [{'to': r['to_id'], 'type': r['rel_type']} for r in rows]

    def query_users(self, filters):
        conn = self._get_conn()
        query = 'SELECT * FROM users WHERE 1=1'
        params = []

        for key, value in filters.items():
            if key in ('name', 'email'):
                query += f' AND {key} LIKE ?'
                params.append(f'%{value}%')
            elif key == 'age':
                query += ' AND age = ?'
                params.append(int(value))

        rows = conn.execute(query, params).fetchall()
        results = []
        for row in rows:
            r = dict(row)
            if r.get('metadata'):
                extra = json.loads(r['metadata'])
                del r['metadata']
                r.update(extra)
            results.append(r)
        return results

