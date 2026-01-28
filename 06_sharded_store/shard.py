import socket
import struct
import threading
import json
import time
from pathlib import Path

class ShardStorage:
    def __init__(self, shard_id, data_dir='shard_data'):
        self.shard_id = shard_id
        self.data_dir = Path(data_dir) / f'shard_{shard_id}'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.data_file = self.data_dir / 'data.json'
        self.data = {}
        self.lock = threading.Lock()
        self._load()

    def _load(self):
        if self.data_file.exists():
            with open(self.data_file) as f:
                self.data = json.load(f)

    def _save(self):
        with open(self.data_file, 'w') as f:
            json.dump(self.data, f)

    def put(self, key, value):
        with self.lock:
            self.data[key] = value
            self._save()

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def delete(self, key):
        with self.lock:
            self.data.pop(key, None)
            self._save()

    def keys(self):
        with self.lock:
            return list(self.data.keys())

    def items(self):
        with self.lock:
            return list(self.data.items())

    def size(self):
        with self.lock:
            return len(self.data)

class ShardServer:
    def __init__(self, shard_id, host='localhost', port=10000, data_dir='shard_data'):
        self.shard_id = shard_id
        self.host = host
        self.port = port
        self.storage = ShardStorage(shard_id, data_dir)
        self.running = False
        self.server_socket = None
        self.server_thread = None
        self.stats = {'gets': 0, 'puts': 0, 'deletes': 0}

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
            self.stats['gets'] += 1
            value = self.storage.get(request['key'])
            return {'ok': True, 'value': value}

        if cmd == 'put':
            self.stats['puts'] += 1
            self.storage.put(request['key'], request['value'])
            return {'ok': True}

        if cmd == 'delete':
            self.stats['deletes'] += 1
            self.storage.delete(request['key'])
            return {'ok': True}

        if cmd == 'keys':
            return {'ok': True, 'keys': self.storage.keys()}

        if cmd == 'bulk_get':
            keys = request.get('keys', [])
            result = {}
            for k in keys:
                v = self.storage.get(k)
                if v is not None:
                    result[k] = v
            return {'ok': True, 'data': result}

        if cmd == 'bulk_put':
            items = request.get('items', {})
            for k, v in items.items():
                self.storage.put(k, v)
            return {'ok': True, 'count': len(items)}

        if cmd == 'bulk_delete':
            keys = request.get('keys', [])
            for k in keys:
                self.storage.delete(k)
            return {'ok': True}

        if cmd == 'status':
            return {
                'ok': True,
                'shard_id': self.shard_id,
                'size': self.storage.size(),
                'stats': self.stats
            }

        if cmd == 'export':
            return {'ok': True, 'data': dict(self.storage.items())}

        if cmd == 'import':
            items = request.get('data', {})
            for k, v in items.items():
                self.storage.put(k, v)
            return {'ok': True, 'imported': len(items)}

        return {'ok': False, 'error': 'unknown command'}

class ShardClient:
    def __init__(self, host, port, timeout=5.0):
        self.host = host
        self.port = port
        self.timeout = timeout

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

    def keys(self):
        return self._send({'cmd': 'keys'})

    def bulk_get(self, keys):
        return self._send({'cmd': 'bulk_get', 'keys': keys})

    def bulk_put(self, items):
        return self._send({'cmd': 'bulk_put', 'items': items})

    def status(self):
        return self._send({'cmd': 'status'})

    def export_data(self):
        return self._send({'cmd': 'export'})

    def import_data(self, data):
        return self._send({'cmd': 'import', 'data': data})

if __name__ == '__main__':
    import sys
    shard_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    port = 10000 + shard_id
    server = ShardServer(shard_id, port=port)
    server.start()
    print(f'shard {shard_id} started on port {port}')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()


