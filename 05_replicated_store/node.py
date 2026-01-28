import socket
import struct
import threading
import json
import time
from pathlib import Path
from enum import IntEnum

class Role(IntEnum):
    FOLLOWER = 0
    LEADER = 1

class OpType(IntEnum):
    PUT = 1
    DELETE = 2

class WALEntry:
    def __init__(self, seq, op_type, key, value=None, timestamp=None):
        self.seq = seq
        self.op_type = op_type
        self.key = key
        self.value = value
        self.timestamp = timestamp or time.time()

    def to_bytes(self):
        key_bytes = self.key.encode('utf-8')
        value_bytes = self.value.encode('utf-8') if self.value else b''
        return struct.pack(
            '>Q B I I d',
            self.seq,
            self.op_type,
            len(key_bytes),
            len(value_bytes),
            self.timestamp
        ) + key_bytes + value_bytes

    @classmethod
    def from_bytes(cls, data, offset=0):
        seq, op_type, key_len, value_len, timestamp = struct.unpack(
            '>Q B I I d', data[offset:offset + 25]
        )
        offset += 25
        key = data[offset:offset + key_len].decode('utf-8')
        offset += key_len
        value = data[offset:offset + value_len].decode('utf-8') if value_len > 0 else None
        return cls(seq, OpType(op_type), key, value, timestamp), offset + value_len

class WAL:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.entries = []
        self.next_seq = 0
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        with open(self.path, 'rb') as f:
            data = f.read()
        offset = 0
        while offset < len(data):
            try:
                entry, offset = WALEntry.from_bytes(data, offset)
                self.entries.append(entry)
                self.next_seq = max(self.next_seq, entry.seq + 1)
            except:
                break

    def append(self, op_type, key, value=None):
        with self.lock:
            entry = WALEntry(self.next_seq, op_type, key, value)
            self.entries.append(entry)
            self.next_seq += 1
            with open(self.path, 'ab') as f:
                f.write(entry.to_bytes())
            return entry

    def get_entries_from(self, seq):
        with self.lock:
            return [e for e in self.entries if e.seq >= seq]

    def get_last_seq(self):
        with self.lock:
            return self.next_seq - 1 if self.entries else -1

class StorageNode:
    def __init__(self, node_id, host='localhost', port=9000, data_dir='repl_data'):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir) / f'node_{node_id}'
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.role = Role.FOLLOWER
        self.leader_id = None
        self.data = {}
        self.wal = WAL(self.data_dir / 'wal.log')
        self.applied_seq = -1

        self.lock = threading.Lock()
        self.running = False
        self.server_socket = None
        self.server_thread = None

        self._replay_wal()

    def _replay_wal(self):
        for entry in self.wal.entries:
            self._apply_entry(entry)

    def _apply_entry(self, entry):
        if entry.seq <= self.applied_seq:
            return
        if entry.op_type == OpType.PUT:
            self.data[entry.key] = entry.value
        elif entry.op_type == OpType.DELETE:
            self.data.pop(entry.key, None)
        self.applied_seq = entry.seq

    def become_leader(self):
        with self.lock:
            self.role = Role.LEADER
            self.leader_id = self.node_id

    def become_follower(self, leader_id):
        with self.lock:
            self.role = Role.FOLLOWER
            self.leader_id = leader_id

    def put(self, key, value):
        with self.lock:
            if self.role != Role.LEADER:
                return False, 'not leader'
            entry = self.wal.append(OpType.PUT, key, value)
            self._apply_entry(entry)
            return True, entry.seq

    def delete(self, key):
        with self.lock:
            if self.role != Role.LEADER:
                return False, 'not leader'
            entry = self.wal.append(OpType.DELETE, key)
            self._apply_entry(entry)
            return True, entry.seq

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def apply_replication(self, entries):
        with self.lock:
            for entry in entries:
                if entry.seq > self.applied_seq:
                    self.wal.entries.append(entry)
                    with open(self.wal.path, 'ab') as f:
                        f.write(entry.to_bytes())
                    self._apply_entry(entry)
            return self.applied_seq

    def get_replication_entries(self, from_seq):
        return self.wal.get_entries_from(from_seq)

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
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
                threading.Thread(target=self._handle_connection, args=(conn,), daemon=True).start()
            except:
                break

    def _handle_connection(self, conn):
        try:
            data = conn.recv(65536)
            if not data:
                return
            request = json.loads(data.decode())
            response = self._handle_request(request)
            conn.sendall(json.dumps(response).encode())
        finally:
            conn.close()

    def _handle_request(self, request):
        cmd = request.get('cmd')

        if cmd == 'get':
            value = self.get(request['key'])
            return {'ok': True, 'value': value}

        if cmd == 'put':
            ok, result = self.put(request['key'], request['value'])
            return {'ok': ok, 'seq': result if ok else None, 'error': result if not ok else None}

        if cmd == 'delete':
            ok, result = self.delete(request['key'])
            return {'ok': ok, 'seq': result if ok else None, 'error': result if not ok else None}

        if cmd == 'repl_fetch':
            from_seq = request.get('from_seq', 0)
            entries = self.get_replication_entries(from_seq)
            return {
                'ok': True,
                'entries': [
                    {'seq': e.seq, 'op': e.op_type, 'key': e.key, 'value': e.value, 'ts': e.timestamp}
                    for e in entries
                ]
            }

        if cmd == 'repl_apply':
            entries = [
                WALEntry(e['seq'], OpType(e['op']), e['key'], e['value'], e['ts'])
                for e in request['entries']
            ]
            applied = self.apply_replication(entries)
            return {'ok': True, 'applied_seq': applied}

        if cmd == 'status':
            return {
                'ok': True,
                'node_id': self.node_id,
                'role': self.role.name,
                'leader_id': self.leader_id,
                'applied_seq': self.applied_seq,
                'wal_seq': self.wal.get_last_seq()
            }

        return {'ok': False, 'error': 'unknown command'}

if __name__ == '__main__':
    import sys
    node_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    port = 9000 + node_id
    node = StorageNode(node_id, port=port)
    if node_id == 0:
        node.become_leader()
    node.start()
    print(f'node {node_id} started on port {port} as {node.role.name}')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()

