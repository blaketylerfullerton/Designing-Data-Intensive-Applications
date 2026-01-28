import threading
from enum import Enum
from store import MVCCStore, ReadUncommittedStore, ReadCommittedStore, SnapshotIsolationStore

class IsolationLevel(Enum):
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SNAPSHOT = 4
    SERIALIZABLE = 5

class TransactionManager:
    def __init__(self, isolation_level=IsolationLevel.SNAPSHOT):
        self.isolation_level = isolation_level

        if isolation_level == IsolationLevel.READ_UNCOMMITTED:
            self.store = ReadUncommittedStore()
        elif isolation_level == IsolationLevel.READ_COMMITTED:
            self.store = ReadCommittedStore()
        else:
            self.store = SnapshotIsolationStore()

    def begin(self):
        return Transaction(self.store.begin_transaction(), self.store, self.isolation_level)

class Transaction:
    def __init__(self, txn_id, store, isolation_level):
        self.txn_id = txn_id
        self.store = store
        self.isolation_level = isolation_level
        self.committed = False
        self.aborted = False

    def read(self, key):
        if self.committed or self.aborted:
            raise RuntimeError('transaction already ended')
        value, error = self.store.read(self.txn_id, key)
        if error:
            raise RuntimeError(error)
        return value

    def write(self, key, value):
        if self.committed or self.aborted:
            raise RuntimeError('transaction already ended')
        success, error = self.store.write(self.txn_id, key, value)
        if not success:
            raise RuntimeError(error)

    def delete(self, key):
        if self.committed or self.aborted:
            raise RuntimeError('transaction already ended')
        success, error = self.store.delete(self.txn_id, key)
        if not success:
            raise RuntimeError(error)

    def commit(self):
        if self.committed or self.aborted:
            raise RuntimeError('transaction already ended')
        success, error = self.store.commit(self.txn_id)
        if success:
            self.committed = True
        else:
            raise RuntimeError(error)

    def abort(self):
        if self.committed or self.aborted:
            raise RuntimeError('transaction already ended')
        self.store.abort(self.txn_id)
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
                self.abort()
                raise
        return False

class TwoPhaseCommit:
    def __init__(self, participants):
        self.participants = participants
        self.coordinator_log = []

    def execute(self, operations):
        txns = {}
        for p_id, participant in self.participants.items():
            txns[p_id] = participant.begin()

        try:
            for op in operations:
                p_id = op['participant']
                txn = txns[p_id]
                if op['type'] == 'write':
                    txn.write(op['key'], op['value'])
                elif op['type'] == 'read':
                    op['result'] = txn.read(op['key'])
        except Exception as e:
            self._abort_all(txns)
            return False, str(e)

        prepared = self._prepare_all(txns)
        if not prepared:
            self._abort_all(txns)
            return False, 'prepare failed'

        self.coordinator_log.append(('commit_decision', list(txns.keys())))

        committed = self._commit_all(txns)
        if not committed:
            return False, 'commit failed'

        return True, operations

    def _prepare_all(self, txns):
        for p_id, txn in txns.items():
            pass
        return True

    def _commit_all(self, txns):
        for p_id, txn in txns.items():
            try:
                txn.commit()
            except:
                return False
        return True

    def _abort_all(self, txns):
        for p_id, txn in txns.items():
            try:
                if not txn.committed and not txn.aborted:
                    txn.abort()
            except:
                pass

class DistributedTransaction:
    def __init__(self, managers):
        self.managers = managers
        self.transactions = {}
        self.active = False

    def begin(self):
        if self.active:
            raise RuntimeError('transaction already active')
        self.transactions = {
            name: mgr.begin() for name, mgr in self.managers.items()
        }
        self.active = True
        return self

    def read(self, partition, key):
        if not self.active:
            raise RuntimeError('transaction not active')
        return self.transactions[partition].read(key)

    def write(self, partition, key, value):
        if not self.active:
            raise RuntimeError('transaction not active')
        self.transactions[partition].write(key, value)

    def commit(self):
        if not self.active:
            raise RuntimeError('transaction not active')

        for txn in self.transactions.values():
            txn.commit()
        self.active = False

    def abort(self):
        if not self.active:
            return
        for txn in self.transactions.values():
            if not txn.committed and not txn.aborted:
                txn.abort()
        self.active = False

    def __enter__(self):
        return self.begin()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.abort()
            return False
        if self.active:
            try:
                self.commit()
            except:
                self.abort()
                raise
        return False

