import socket
import json
import threading
import time

class ReplicationClient:
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
            response = sock.recv(65536)
            sock.close()
            return json.loads(response.decode())
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def fetch_entries(self, from_seq):
        return self._send({'cmd': 'repl_fetch', 'from_seq': from_seq})

    def apply_entries(self, entries):
        return self._send({'cmd': 'repl_apply', 'entries': entries})

    def get_status(self):
        return self._send({'cmd': 'status'})

class ReplicationManager:
    def __init__(self, leader_node, follower_addrs, sync_interval=1.0):
        self.leader = leader_node
        self.followers = {
            addr: {'client': ReplicationClient(addr[0], addr[1]), 'last_seq': -1}
            for addr in follower_addrs
        }
        self.sync_interval = sync_interval
        self.running = False
        self.thread = None
        self.lock = threading.Lock()

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._sync_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _sync_loop(self):
        while self.running:
            self._sync_all()
            time.sleep(self.sync_interval)

    def _sync_all(self):
        leader_seq = self.leader.wal.get_last_seq()

        for addr, state in self.followers.items():
            if state['last_seq'] < leader_seq:
                self._sync_follower(addr, state)

    def _sync_follower(self, addr, state):
        client = state['client']
        from_seq = state['last_seq'] + 1
        entries = self.leader.get_replication_entries(from_seq)

        if not entries:
            return

        entries_data = [
            {'seq': e.seq, 'op': e.op_type, 'key': e.key, 'value': e.value, 'ts': e.timestamp}
            for e in entries
        ]

        response = client.apply_entries(entries_data)

        if response.get('ok'):
            state['last_seq'] = response.get('applied_seq', state['last_seq'])

    def get_replication_lag(self):
        leader_seq = self.leader.wal.get_last_seq()
        lag = {}
        for addr, state in self.followers.items():
            lag[f'{addr[0]}:{addr[1]}'] = leader_seq - state['last_seq']
        return lag

    def wait_for_replication(self, seq, timeout=10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            all_caught_up = True
            for addr, state in self.followers.items():
                if state['last_seq'] < seq:
                    all_caught_up = False
                    break
            if all_caught_up:
                return True
            time.sleep(0.1)
        return False

class AsyncReplicator:
    def __init__(self, source_client, dest_client, poll_interval=0.5):
        self.source = source_client
        self.dest = dest_client
        self.poll_interval = poll_interval
        self.last_seq = -1
        self.running = False
        self.thread = None
        self.stats = {'synced': 0, 'errors': 0}

    def start(self):
        status = self.dest.get_status()
        if status.get('ok'):
            self.last_seq = status.get('applied_seq', -1)

        self.running = True
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _poll_loop(self):
        while self.running:
            self._poll_and_apply()
            time.sleep(self.poll_interval)

    def _poll_and_apply(self):
        response = self.source.fetch_entries(self.last_seq + 1)

        if not response.get('ok'):
            self.stats['errors'] += 1
            return

        entries = response.get('entries', [])
        if not entries:
            return

        apply_response = self.dest.apply_entries(entries)

        if apply_response.get('ok'):
            self.last_seq = apply_response.get('applied_seq', self.last_seq)
            self.stats['synced'] += len(entries)
        else:
            self.stats['errors'] += 1

class SyncReplicator:
    def __init__(self, leader_node, follower_clients, quorum=None):
        self.leader = leader_node
        self.followers = follower_clients
        self.quorum = quorum or (len(follower_clients) // 2 + 1)

    def replicate_sync(self, entry):
        entries_data = [
            {'seq': entry.seq, 'op': entry.op_type, 'key': entry.key, 'value': entry.value, 'ts': entry.timestamp}
        ]

        acks = 1
        for client in self.followers:
            response = client.apply_entries(entries_data)
            if response.get('ok'):
                acks += 1
            if acks >= self.quorum:
                return True

        return acks >= self.quorum

