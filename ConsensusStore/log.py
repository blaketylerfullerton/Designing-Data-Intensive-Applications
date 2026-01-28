import struct
import json
import threading
from pathlib import Path
from dataclasses import dataclass
from typing import Any, List, Optional

@dataclass
class PersistentLogEntry:
    term: int
    index: int
    command: Any

class PersistentLog:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.entries = []
        self.lock = threading.Lock()
        self._load()

    def _load(self):
        if not self.path.exists():
            return

        with open(self.path, 'rb') as f:
            while True:
                header = f.read(16)
                if len(header) < 16:
                    break

                term, index, cmd_len = struct.unpack('>Q Q', header[:16])

                header_rest = f.read(4)
                if len(header_rest) < 4:
                    break
                cmd_len = struct.unpack('>I', header_rest)[0]

                cmd_data = f.read(cmd_len)
                if len(cmd_data) < cmd_len:
                    break

                command = json.loads(cmd_data.decode())
                self.entries.append(PersistentLogEntry(term, index, command))

    def _append_to_file(self, entry):
        cmd_bytes = json.dumps(entry.command).encode()
        header = struct.pack('>Q Q I', entry.term, entry.index, len(cmd_bytes))

        with open(self.path, 'ab') as f:
            f.write(header)
            f.write(cmd_bytes)

    def append(self, term, command):
        with self.lock:
            index = len(self.entries) + 1
            entry = PersistentLogEntry(term, index, command)
            self.entries.append(entry)
            self._append_to_file(entry)
            return entry

    def get(self, index):
        with self.lock:
            if 1 <= index <= len(self.entries):
                return self.entries[index - 1]
            return None

    def get_range(self, start_index, end_index=None):
        with self.lock:
            if end_index is None:
                end_index = len(self.entries)
            return self.entries[start_index - 1:end_index]

    def last_index(self):
        with self.lock:
            return len(self.entries)

    def last_term(self):
        with self.lock:
            if self.entries:
                return self.entries[-1].term
            return 0

    def truncate_from(self, index):
        with self.lock:
            if index <= len(self.entries):
                self.entries = self.entries[:index - 1]
                self._rewrite()

    def _rewrite(self):
        with open(self.path, 'wb') as f:
            for entry in self.entries:
                cmd_bytes = json.dumps(entry.command).encode()
                header = struct.pack('>Q Q I', entry.term, entry.index, len(cmd_bytes))
                f.write(header)
                f.write(cmd_bytes)

class RaftMetadata:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.current_term = 0
        self.voted_for = None
        self.lock = threading.Lock()
        self._load()

    def _load(self):
        if not self.path.exists():
            return

        with open(self.path, 'rb') as f:
            data = f.read()
            if len(data) >= 16:
                term, voted_for = struct.unpack('>Q q', data[:16])
                self.current_term = term
                self.voted_for = voted_for if voted_for >= 0 else None

    def _save(self):
        voted = self.voted_for if self.voted_for is not None else -1
        with open(self.path, 'wb') as f:
            f.write(struct.pack('>Q q', self.current_term, voted))

    def get_term(self):
        with self.lock:
            return self.current_term

    def get_voted_for(self):
        with self.lock:
            return self.voted_for

    def set_term(self, term):
        with self.lock:
            self.current_term = term
            self.voted_for = None
            self._save()

    def set_voted_for(self, node_id):
        with self.lock:
            self.voted_for = node_id
            self._save()

    def update(self, term, voted_for):
        with self.lock:
            self.current_term = term
            self.voted_for = voted_for
            self._save()

class LogCompactor:
    def __init__(self, log, snapshot_path, compact_threshold=1000):
        self.log = log
        self.snapshot_path = Path(snapshot_path)
        self.compact_threshold = compact_threshold
        self.last_included_index = 0
        self.last_included_term = 0

    def should_compact(self):
        return self.log.last_index() - self.last_included_index > self.compact_threshold

    def create_snapshot(self, state_machine_state, last_applied):
        if last_applied <= self.last_included_index:
            return False

        entry = self.log.get(last_applied)
        if not entry:
            return False

        snapshot = {
            'last_included_index': last_applied,
            'last_included_term': entry.term,
            'state': state_machine_state
        }

        with open(self.snapshot_path, 'w') as f:
            json.dump(snapshot, f)

        self.last_included_index = last_applied
        self.last_included_term = entry.term

        return True

    def load_snapshot(self):
        if not self.snapshot_path.exists():
            return None

        with open(self.snapshot_path) as f:
            snapshot = json.load(f)

        self.last_included_index = snapshot['last_included_index']
        self.last_included_term = snapshot['last_included_term']

        return snapshot

    def get_snapshot_metadata(self):
        return {
            'last_included_index': self.last_included_index,
            'last_included_term': self.last_included_term
        }


