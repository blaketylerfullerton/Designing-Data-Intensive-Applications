import threading
from log_store import LogStore
from compaction import Compactor
from index import HashIndex, BloomFilter, SSTableIndex

class StorageEngine:
    def __init__(self, data_dir='engine_data'):
        self.data_dir = data_dir
        self.log_store = LogStore(data_dir)
        self.index = HashIndex()
        self.bloom = BloomFilter(size=100000, num_hashes=4)
        self.compactor = Compactor(self.log_store, interval=30.0)
        self.lock = threading.Lock()
        self._rebuild_index()

    def _rebuild_index(self):
        self.index.rebuild(self.log_store)
        self.bloom.rebuild(self.index.keys())

    def put(self, key, value):
        with self.lock:
            segment_id, offset = self.log_store.put(key, value)
            self.index.put(key, segment_id, offset)
            self.bloom.add(key)

    def get(self, key):
        if not self.bloom.might_contain(key):
            return None

        location = self.index.get(key)
        if location is None:
            return None

        segment_id, offset = location
        segment = self.log_store.get_segment(segment_id)
        if segment is None:
            return None

        key_read, value, deleted = segment.read_at(offset)
        if deleted or key_read != key:
            return None

        return value

    def delete(self, key):
        with self.lock:
            self.log_store.delete(key)
            self.index.delete(key)

    def exists(self, key):
        return self.bloom.might_contain(key) and self.index.get(key) is not None

    def keys(self):
        return self.index.keys()

    def start_compaction(self):
        self.compactor.start()

    def stop_compaction(self):
        self.compactor.stop()

    def force_compaction(self):
        return self.compactor.compact()

    def stats(self):
        return {
            'segments': len(self.log_store.segments),
            'keys': len(self.index.keys()),
            'total_size': sum(s.size for s in self.log_store.segments)
        }

class LSMTree:
    def __init__(self, data_dir='lsm_data', memtable_size=1000):
        self.data_dir = data_dir
        self.memtable_size = memtable_size
        self.memtable = {}
        self.immutable_memtable = None
        self.sstables = []
        self.lock = threading.Lock()
        self.flush_lock = threading.Lock()

    def put(self, key, value):
        with self.lock:
            self.memtable[key] = value
            if len(self.memtable) >= self.memtable_size:
                self._freeze_memtable()

    def _freeze_memtable(self):
        with self.flush_lock:
            self.immutable_memtable = self.memtable
            self.memtable = {}
            self._flush_to_sstable()

    def _flush_to_sstable(self):
        if not self.immutable_memtable:
            return

        sstable_id = len(self.sstables)
        path = f'{self.data_dir}/sstable_{sstable_id:06d}'
        sstable = SSTableIndex(path)
        sstable.build_from_records(self.immutable_memtable)
        self.sstables.append(sstable)
        self.immutable_memtable = None

    def get(self, key):
        with self.lock:
            if key in self.memtable:
                value = self.memtable[key]
                return None if value is None else value

        if self.immutable_memtable and key in self.immutable_memtable:
            value = self.immutable_memtable[key]
            return None if value is None else value

        for sstable in reversed(self.sstables):
            value = sstable.get(key)
            if value is not None:
                return value

        return None

    def delete(self, key):
        self.put(key, None)

    def range_query(self, start_key, end_key):
        results = {}

        for sstable in self.sstables:
            for k, v in sstable.range_scan(start_key, end_key):
                if k not in results:
                    results[k] = v

        if self.immutable_memtable:
            for k, v in self.immutable_memtable.items():
                if start_key <= k <= end_key:
                    results[k] = v

        with self.lock:
            for k, v in self.memtable.items():
                if start_key <= k <= end_key:
                    results[k] = v

        return [(k, v) for k, v in sorted(results.items()) if v is not None]

if __name__ == '__main__':
    engine = StorageEngine('test_engine')

    for i in range(1000):
        engine.put(f'key_{i:04d}', f'value_{i}')

    print(f'stats: {engine.stats()}')

    for i in range(0, 1000, 100):
        value = engine.get(f'key_{i:04d}')
        print(f'key_{i:04d} = {value}')

    engine.delete('key_0500')
    print(f'after delete: key_0500 = {engine.get("key_0500")}')

    lsm = LSMTree('test_lsm', memtable_size=100)
    for i in range(500):
        lsm.put(f'lsm_key_{i:04d}', f'lsm_value_{i}')

    print(f'lsm get: lsm_key_0250 = {lsm.get("lsm_key_0250")}')
    print(f'lsm range: {lsm.range_query("lsm_key_0100", "lsm_key_0110")}')

