import socket
import json
import threading
import time
import random
from enum import IntEnum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

class State(IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

@dataclass
class LogEntry:
    term: int
    index: int
    command: Any

@dataclass
class RaftState:
    current_term: int = 0
    voted_for: Optional[int] = None
    log: List[LogEntry] = field(default_factory=list)
    commit_index: int = 0
    last_applied: int = 0

class RaftNode:
    def __init__(self, node_id, peers, host='localhost', port=16000):
        self.node_id = node_id
        self.peers = peers
        self.host = host
        self.port = port

        self.state = State.FOLLOWER
        self.raft_state = RaftState()
        self.next_index = {}
        self.match_index = {}

        self.lock = threading.Lock()
        self.running = False
        self.leader_id = None
        self.election_timeout = self._random_timeout()
        self.last_heartbeat = time.time()

        self.apply_callback = None
        self.server_socket = None

    def _random_timeout(self):
        return random.uniform(1.5, 3.0)

    def _reset_election_timer(self):
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_timeout()

    def start(self):
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)

        threading.Thread(target=self._serve, daemon=True).start()
        threading.Thread(target=self._election_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

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

        if cmd == 'request_vote':
            return self._handle_request_vote(request)
        if cmd == 'append_entries':
            return self._handle_append_entries(request)
        if cmd == 'client_request':
            return self._handle_client_request(request)
        if cmd == 'status':
            return self._get_status()

        return {'ok': False, 'error': 'unknown command'}

    def _handle_request_vote(self, request):
        with self.lock:
            term = request['term']
            candidate_id = request['candidate_id']
            last_log_index = request['last_log_index']
            last_log_term = request['last_log_term']

            if term > self.raft_state.current_term:
                self.raft_state.current_term = term
                self.raft_state.voted_for = None
                self.state = State.FOLLOWER

            if term < self.raft_state.current_term:
                return {'term': self.raft_state.current_term, 'vote_granted': False}

            log_ok = True
            if self.raft_state.log:
                my_last_term = self.raft_state.log[-1].term
                my_last_index = self.raft_state.log[-1].index
                if last_log_term < my_last_term:
                    log_ok = False
                elif last_log_term == my_last_term and last_log_index < my_last_index:
                    log_ok = False

            if log_ok and (self.raft_state.voted_for is None or self.raft_state.voted_for == candidate_id):
                self.raft_state.voted_for = candidate_id
                self._reset_election_timer()
                return {'term': self.raft_state.current_term, 'vote_granted': True}

            return {'term': self.raft_state.current_term, 'vote_granted': False}

    def _handle_append_entries(self, request):
        with self.lock:
            term = request['term']
            leader_id = request['leader_id']
            prev_log_index = request['prev_log_index']
            prev_log_term = request['prev_log_term']
            entries = request['entries']
            leader_commit = request['leader_commit']

            if term > self.raft_state.current_term:
                self.raft_state.current_term = term
                self.raft_state.voted_for = None
                self.state = State.FOLLOWER

            if term < self.raft_state.current_term:
                return {'term': self.raft_state.current_term, 'success': False}

            self._reset_election_timer()
            self.leader_id = leader_id
            self.state = State.FOLLOWER

            if prev_log_index > 0:
                if prev_log_index > len(self.raft_state.log):
                    return {'term': self.raft_state.current_term, 'success': False}
                if self.raft_state.log[prev_log_index - 1].term != prev_log_term:
                    self.raft_state.log = self.raft_state.log[:prev_log_index - 1]
                    return {'term': self.raft_state.current_term, 'success': False}

            for entry_data in entries:
                entry = LogEntry(entry_data['term'], entry_data['index'], entry_data['command'])
                if entry.index <= len(self.raft_state.log):
                    self.raft_state.log[entry.index - 1] = entry
                else:
                    self.raft_state.log.append(entry)

            if leader_commit > self.raft_state.commit_index:
                self.raft_state.commit_index = min(leader_commit, len(self.raft_state.log))
                self._apply_committed()

            return {'term': self.raft_state.current_term, 'success': True}

    def _handle_client_request(self, request):
        with self.lock:
            if self.state != State.LEADER:
                return {'ok': False, 'error': 'not leader', 'leader_id': self.leader_id}

            command = request['command']
            entry = LogEntry(
                term=self.raft_state.current_term,
                index=len(self.raft_state.log) + 1,
                command=command
            )
            self.raft_state.log.append(entry)

            return {'ok': True, 'index': entry.index}

    def _get_status(self):
        with self.lock:
            return {
                'ok': True,
                'node_id': self.node_id,
                'state': self.state.name,
                'term': self.raft_state.current_term,
                'leader_id': self.leader_id,
                'log_length': len(self.raft_state.log),
                'commit_index': self.raft_state.commit_index,
                'last_applied': self.raft_state.last_applied
            }

    def _election_loop(self):
        while self.running:
            time.sleep(0.1)
            with self.lock:
                if self.state == State.LEADER:
                    continue
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self._start_election()

    def _start_election(self):
        self.state = State.CANDIDATE
        self.raft_state.current_term += 1
        self.raft_state.voted_for = self.node_id
        self._reset_election_timer()

        votes = 1
        term = self.raft_state.current_term

        last_log_index = len(self.raft_state.log)
        last_log_term = self.raft_state.log[-1].term if self.raft_state.log else 0

        for peer_id, (host, port) in self.peers.items():
            response = self._send_rpc(host, port, {
                'cmd': 'request_vote',
                'term': term,
                'candidate_id': self.node_id,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            })

            if response and response.get('vote_granted'):
                votes += 1

            if response and response.get('term', 0) > term:
                self.raft_state.current_term = response['term']
                self.state = State.FOLLOWER
                self.raft_state.voted_for = None
                return

        if votes > len(self.peers) // 2 and self.state == State.CANDIDATE:
            self._become_leader()

    def _become_leader(self):
        self.state = State.LEADER
        self.leader_id = self.node_id

        for peer_id in self.peers:
            self.next_index[peer_id] = len(self.raft_state.log) + 1
            self.match_index[peer_id] = 0

    def _heartbeat_loop(self):
        while self.running:
            time.sleep(0.5)
            with self.lock:
                if self.state == State.LEADER:
                    self._send_heartbeats()

    def _send_heartbeats(self):
        for peer_id, (host, port) in self.peers.items():
            self._replicate_to_peer(peer_id, host, port)

    def _replicate_to_peer(self, peer_id, host, port):
        prev_log_index = self.next_index.get(peer_id, 1) - 1
        prev_log_term = 0
        if prev_log_index > 0 and prev_log_index <= len(self.raft_state.log):
            prev_log_term = self.raft_state.log[prev_log_index - 1].term

        entries = []
        for i in range(self.next_index.get(peer_id, 1) - 1, len(self.raft_state.log)):
            entry = self.raft_state.log[i]
            entries.append({'term': entry.term, 'index': entry.index, 'command': entry.command})

        response = self._send_rpc(host, port, {
            'cmd': 'append_entries',
            'term': self.raft_state.current_term,
            'leader_id': self.node_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit': self.raft_state.commit_index
        })

        if response:
            if response.get('term', 0) > self.raft_state.current_term:
                self.raft_state.current_term = response['term']
                self.state = State.FOLLOWER
                self.raft_state.voted_for = None
                return

            if response.get('success'):
                if entries:
                    self.next_index[peer_id] = entries[-1]['index'] + 1
                    self.match_index[peer_id] = entries[-1]['index']
                    self._update_commit_index()
            else:
                self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)

    def _update_commit_index(self):
        for n in range(self.raft_state.commit_index + 1, len(self.raft_state.log) + 1):
            if self.raft_state.log[n - 1].term != self.raft_state.current_term:
                continue

            replicated = 1
            for peer_id in self.peers:
                if self.match_index.get(peer_id, 0) >= n:
                    replicated += 1

            if replicated > len(self.peers) // 2:
                self.raft_state.commit_index = n

        self._apply_committed()

    def _apply_committed(self):
        while self.raft_state.last_applied < self.raft_state.commit_index:
            self.raft_state.last_applied += 1
            entry = self.raft_state.log[self.raft_state.last_applied - 1]
            if self.apply_callback:
                self.apply_callback(entry)

    def _send_rpc(self, host, port, request):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((host, port))
            sock.sendall(json.dumps(request).encode())
            response = json.loads(sock.recv(65536).decode())
            sock.close()
            return response
        except:
            return None

if __name__ == '__main__':
    import sys
    node_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    port = 16000 + node_id

    peers = {}
    for i in range(3):
        if i != node_id:
            peers[i] = ('localhost', 16000 + i)

    node = RaftNode(node_id, peers, port=port)
    node.start()
    print(f'raft node {node_id} started on port {port}')

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()

