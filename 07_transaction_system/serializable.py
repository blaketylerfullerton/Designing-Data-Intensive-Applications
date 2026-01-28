import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Set, Dict, Optional
from store import MVCCStore

@dataclass
class SSITransaction:
    txn_id: int
    start_ts: float
    commit_ts: Optional[float] = None
    read_set: Set[str] = field(default_factory=set)
    write_set: Dict[str, any] = field(default_factory=dict)
    in_conflict: Set[int] = field(default_factory=set)
    out_conflict: Set[int] = field(default_factory=set)
    committed: bool = False
    aborted: bool = False

class SerializableSnapshotIsolation:
    def __init__(self):
        self.store = MVCCStore()
        self.transactions: Dict[int, SSITransaction] = {}
        self.committed_txns: Dict[int, SSITransaction] = {}
        self.siread_locks: Dict[str, Set[int]] = defaultdict(set)
        self.write_locks: Dict[str, int] = {}
        self.lock = threading.Lock()
        self.next_txn_id = 1

    def begin(self):
        with self.lock:
            txn_id = self.next_txn_id
            self.next_txn_id += 1
            txn = SSITransaction(txn_id=txn_id, start_ts=time.time())
            self.transactions[txn_id] = txn
            self.store.active_txns[txn_id] = {
                'start_ts': txn.start_ts,
                'read_set': set(),
                'write_set': {},
                'status': 'active'
            }
            return txn_id

    def read(self, txn_id, key):
        with self.lock:
            txn = self.transactions.get(txn_id)
            if not txn or txn.aborted or txn.committed:
                return None, 'invalid transaction'

            if key in txn.write_set:
                return txn.write_set[key], None

            txn.read_set.add(key)
            self.siread_locks[key].add(txn_id)

            if key in self.write_locks:
                writer_id = self.write_locks[key]
                if writer_id != txn_id and writer_id in self.transactions:
                    writer_txn = self.transactions[writer_id]
                    if not writer_txn.committed:
                        txn.in_conflict.add(writer_id)
                        writer_txn.out_conflict.add(txn_id)

            value, error = self.store.read(txn_id, key)
            return value, error

    def write(self, txn_id, key, value):
        with self.lock:
            txn = self.transactions.get(txn_id)
            if not txn or txn.aborted or txn.committed:
                return False, 'invalid transaction'

            if key in self.write_locks and self.write_locks[key] != txn_id:
                return False, 'write conflict'

            for reader_id in self.siread_locks.get(key, set()):
                if reader_id != txn_id and reader_id in self.transactions:
                    reader_txn = self.transactions[reader_id]
                    if not reader_txn.aborted:
                        txn.in_conflict.add(reader_id)
                        reader_txn.out_conflict.add(txn_id)

            self.write_locks[key] = txn_id
            txn.write_set[key] = value

            success, error = self.store.write(txn_id, key, value)
            return success, error

    def commit(self, txn_id):
        with self.lock:
            txn = self.transactions.get(txn_id)
            if not txn or txn.aborted or txn.committed:
                return False, 'invalid transaction'

            if self._has_dangerous_structure(txn):
                self._abort_internal(txn)
                return False, 'serialization failure'

            txn.commit_ts = time.time()
            success, error = self.store.commit(txn_id)

            if success:
                txn.committed = True
                self.committed_txns[txn_id] = txn
                self._cleanup_locks(txn)

            return success, error

    def abort(self, txn_id):
        with self.lock:
            txn = self.transactions.get(txn_id)
            if not txn:
                return False, 'invalid transaction'
            self._abort_internal(txn)
            return True, None

    def _abort_internal(self, txn):
        txn.aborted = True
        self.store.abort(txn.txn_id)
        self._cleanup_locks(txn)

    def _cleanup_locks(self, txn):
        for key in txn.read_set:
            self.siread_locks[key].discard(txn.txn_id)

        for key in txn.write_set:
            if self.write_locks.get(key) == txn.txn_id:
                del self.write_locks[key]

    def _has_dangerous_structure(self, txn):
        for in_txn_id in txn.in_conflict:
            in_txn = self.transactions.get(in_txn_id) or self.committed_txns.get(in_txn_id)
            if not in_txn:
                continue
            if in_txn.committed and in_txn.commit_ts < txn.start_ts:
                continue

            for out_txn_id in txn.out_conflict:
                out_txn = self.transactions.get(out_txn_id) or self.committed_txns.get(out_txn_id)
                if not out_txn:
                    continue
                if out_txn.committed and out_txn.commit_ts < txn.start_ts:
                    continue

                if not in_txn.aborted and not out_txn.aborted:
                    return True

        return False

class SSITransactionWrapper:
    def __init__(self, txn_id, ssi):
        self.txn_id = txn_id
        self.ssi = ssi
        self.committed = False
        self.aborted = False

    def read(self, key):
        value, error = self.ssi.read(self.txn_id, key)
        if error:
            raise RuntimeError(error)
        return value

    def write(self, key, value):
        success, error = self.ssi.write(self.txn_id, key, value)
        if not success:
            raise RuntimeError(error)

    def commit(self):
        success, error = self.ssi.commit(self.txn_id)
        if success:
            self.committed = True
        else:
            self.aborted = True
            raise RuntimeError(error)

    def abort(self):
        self.ssi.abort(self.txn_id)
        self.aborted = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            if not self.aborted and not self.committed:
                self.abort()
            return False
        if not self.committed and not self.aborted:
            try:
                self.commit()
            except:
                if not self.aborted:
                    self.abort()
                raise
        return False

class SSIManager:
    def __init__(self):
        self.ssi = SerializableSnapshotIsolation()

    def begin(self):
        txn_id = self.ssi.begin()
        return SSITransactionWrapper(txn_id, self.ssi)

def test_ssi_prevents_write_skew():
    print('=== SSI prevents write skew ===')
    mgr = SSIManager()

    setup = mgr.begin()
    setup.write('alice_balance', 100)
    setup.write('bob_balance', 100)
    setup.commit()

    barrier = threading.Barrier(2)
    results = []

    def withdraw_alice():
        t = mgr.begin()
        try:
            alice = t.read('alice_balance')
            bob = t.read('bob_balance')
            total = alice + bob
            barrier.wait()
            if total >= 150:
                t.write('alice_balance', alice - 150)
            t.commit()
            results.append(('alice', 'committed'))
        except Exception as e:
            results.append(('alice', f'aborted: {e}'))

    def withdraw_bob():
        t = mgr.begin()
        try:
            alice = t.read('alice_balance')
            bob = t.read('bob_balance')
            total = alice + bob
            barrier.wait()
            if total >= 150:
                t.write('bob_balance', bob - 150)
            t.commit()
            results.append(('bob', 'committed'))
        except Exception as e:
            results.append(('bob', f'aborted: {e}'))

    threads = [threading.Thread(target=withdraw_alice), threading.Thread(target=withdraw_bob)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = mgr.begin()
    alice_final = final.read('alice_balance')
    bob_final = final.read('bob_balance')
    final.commit()

    print(f'results: {results}')
    print(f'final: alice={alice_final}, bob={bob_final}, total={alice_final + bob_final}')
    print(f'invariant preserved: {alice_final + bob_final >= 0}')

if __name__ == '__main__':
    test_ssi_prevents_write_skew()

