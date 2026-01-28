import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, Any, List, Dict

@dataclass
class Version:
    value: Any
    txn_id: int
    timestamp: float
    deleted: bool = False

@dataclass
class MVCCRecord:
    versions: List[Version] = field(default_factory=list)
    lock_holder: Optional[int] = None
    lock_mode: Optional[str] = None

class MVCCStore:
    def __init__(self):
        self.data: Dict[str, MVCCRecord] = defaultdict(MVCCRecord)
        self.lock = threading.Lock()
        self.next_txn_id = 1
        self.active_txns = {}
        self.committed_txns = {}

    def begin_transaction(self):
        with self.lock:
            txn_id = self.next_txn_id
            self.next_txn_id += 1
            snapshot_ts = time.time()
            self.active_txns[txn_id] = {
                'start_ts': snapshot_ts,
                'read_set': set(),
                'write_set': {},
                'status': 'active'
            }
            return txn_id

    def _get_visible_version(self, key, txn_id):
        record = self.data.get(key)
        if not record or not record.versions:
            return None

        txn_info = self.active_txns.get(txn_id)
        snapshot_ts = txn_info['start_ts'] if txn_info else time.time()

        for version in reversed(record.versions):
            if version.txn_id == txn_id:
                return None if version.deleted else version.value

            if version.txn_id in self.committed_txns:
                commit_ts = self.committed_txns[version.txn_id]
                if commit_ts <= snapshot_ts:
                    return None if version.deleted else version.value

        return None

    def read(self, txn_id, key):
        with self.lock:
            if txn_id not in self.active_txns:
                return None, 'transaction not active'

            txn_info = self.active_txns[txn_id]

            if key in txn_info['write_set']:
                pending = txn_info['write_set'][key]
                return (None if pending['deleted'] else pending['value']), None

            txn_info['read_set'].add(key)
            value = self._get_visible_version(key, txn_id)
            return value, None

    def write(self, txn_id, key, value):
        with self.lock:
            if txn_id not in self.active_txns:
                return False, 'transaction not active'

            record = self.data[key]

            if record.lock_holder is not None and record.lock_holder != txn_id:
                return False, 'write conflict'

            record.lock_holder = txn_id
            record.lock_mode = 'write'

            self.active_txns[txn_id]['write_set'][key] = {
                'value': value,
                'deleted': False
            }
            return True, None

    def delete(self, txn_id, key):
        with self.lock:
            if txn_id not in self.active_txns:
                return False, 'transaction not active'

            record = self.data[key]

            if record.lock_holder is not None and record.lock_holder != txn_id:
                return False, 'write conflict'

            record.lock_holder = txn_id
            record.lock_mode = 'write'

            self.active_txns[txn_id]['write_set'][key] = {
                'value': None,
                'deleted': True
            }
            return True, None

    def commit(self, txn_id):
        with self.lock:
            if txn_id not in self.active_txns:
                return False, 'transaction not active'

            txn_info = self.active_txns[txn_id]
            commit_ts = time.time()

            for key, pending in txn_info['write_set'].items():
                record = self.data[key]
                version = Version(
                    value=pending['value'],
                    txn_id=txn_id,
                    timestamp=commit_ts,
                    deleted=pending['deleted']
                )
                record.versions.append(version)
                record.lock_holder = None
                record.lock_mode = None

            self.committed_txns[txn_id] = commit_ts
            del self.active_txns[txn_id]
            return True, None

    def abort(self, txn_id):
        with self.lock:
            if txn_id not in self.active_txns:
                return False, 'transaction not active'

            txn_info = self.active_txns[txn_id]

            for key in txn_info['write_set']:
                record = self.data[key]
                if record.lock_holder == txn_id:
                    record.lock_holder = None
                    record.lock_mode = None

            del self.active_txns[txn_id]
            return True, None

    def gc_old_versions(self, before_ts):
        with self.lock:
            for key, record in self.data.items():
                visible = []
                for version in record.versions:
                    if version.txn_id in self.committed_txns:
                        if self.committed_txns[version.txn_id] >= before_ts:
                            visible.append(version)
                    else:
                        visible.append(version)
                if len(visible) < len(record.versions):
                    if visible:
                        record.versions = visible
                    else:
                        record.versions = record.versions[-1:]

class ReadUncommittedStore(MVCCStore):
    def read(self, txn_id, key):
        with self.lock:
            if txn_id not in self.active_txns:
                return None, 'transaction not active'

            record = self.data.get(key)
            if not record or not record.versions:
                return None, None

            latest = record.versions[-1]
            return None if latest.deleted else latest.value, None

class ReadCommittedStore(MVCCStore):
    def read(self, txn_id, key):
        with self.lock:
            if txn_id not in self.active_txns:
                return None, 'transaction not active'

            txn_info = self.active_txns[txn_id]

            if key in txn_info['write_set']:
                pending = txn_info['write_set'][key]
                return (None if pending['deleted'] else pending['value']), None

            record = self.data.get(key)
            if not record or not record.versions:
                return None, None

            for version in reversed(record.versions):
                if version.txn_id in self.committed_txns:
                    return None if version.deleted else version.value, None

            return None, None

class SnapshotIsolationStore(MVCCStore):
    pass


