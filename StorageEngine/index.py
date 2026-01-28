import bisect
import struct
import threading
from pathlib import Path

class HashIndex:
    def __init__(self):
        self.index = {}
        self.lock = threading.RLock()

    def put(self, key, segment_id, offset):
        with self.lock:
            self.index[key] = (segment_id, offset)

    def get(self, key):
        with self.lock:
            return self.index.get(key)

    def delete(self, key):
        with self.lock:
            self.index.pop(key, None)

    def keys(self):
        with self.lock:
            return list(self.index.keys())

    def rebuild(self, log_store):
        with self.lock:
            self.index.clear()
            for segment in log_store.segments:
                for offset, key, value, deleted in segment.iterate():
                    if deleted:
                        self.index.pop(key, None)
                    else:
                        self.index[key] = (segment.segment_id, offset)

class SparseIndex:
    def __init__(self, interval=100):
        self.entries = []
        self.interval = interval
        self.lock = threading.RLock()

    def add(self, key, segment_id, offset):
        with self.lock:
            entry = (key, segment_id, offset)
            idx = bisect.bisect_left(self.entries, entry)
            if idx < len(self.entries) and self.entries[idx][0] == key:
                self.entries[idx] = entry
            else:
                self.entries.insert(idx, entry)

    def find_range(self, key):
        with self.lock:
            if not self.entries:
                return None, None

            idx = bisect.bisect_right(self.entries, (key,)) - 1
            if idx < 0:
                return None, self.entries[0] if self.entries else None

            start = self.entries[idx]
            end = self.entries[idx + 1] if idx + 1 < len(self.entries) else None
            return start, end

    def rebuild(self, log_store):
        with self.lock:
            self.entries.clear()
            count = 0
            for segment in log_store.segments:
                for offset, key, value, deleted in segment.iterate():
                    if not deleted and count % self.interval == 0:
                        self.entries.append((key, segment.segment_id, offset))
                    count += 1
            self.entries.sort()

class SSTableIndex:
    def __init__(self, path):
        self.path = Path(path)
        self.data_path = self.path.with_suffix('.data')
        self.index_path = self.path.with_suffix('.index')
        self.sparse_index = []
        self.lock = threading.RLock()

    def build_from_records(self, records, sparse_interval=100):
        sorted_keys = sorted(records.keys())

        with open(self.data_path, 'wb') as data_file:
            offset = 0
            count = 0
            for key in sorted_keys:
                value = records[key]
                key_bytes = key.encode('utf-8')
                value_bytes = value.encode('utf-8')

                record = struct.pack('>I I', len(key_bytes), len(value_bytes))
                record += key_bytes + value_bytes

                if count % sparse_interval == 0:
                    self.sparse_index.append((key, offset))

                data_file.write(record)
                offset += len(record)
                count += 1

        with open(self.index_path, 'wb') as index_file:
            for key, offset in self.sparse_index:
                key_bytes = key.encode('utf-8')
                index_file.write(struct.pack('>I Q', len(key_bytes), offset))
                index_file.write(key_bytes)

    def load_index(self):
        if not self.index_path.exists():
            return

        with self.lock:
            self.sparse_index.clear()
            with open(self.index_path, 'rb') as f:
                while True:
                    header = f.read(12)
                    if len(header) < 12:
                        break
                    key_len, offset = struct.unpack('>I Q', header)
                    key = f.read(key_len).decode('utf-8')
                    self.sparse_index.append((key, offset))

    def get(self, key):
        with self.lock:
            if not self.sparse_index:
                return None

            idx = bisect.bisect_right(self.sparse_index, (key,)) - 1
            if idx < 0:
                start_offset = 0
            else:
                start_offset = self.sparse_index[idx][1]

            if idx + 1 < len(self.sparse_index):
                end_offset = self.sparse_index[idx + 1][1]
            else:
                end_offset = self.data_path.stat().st_size if self.data_path.exists() else 0

        if not self.data_path.exists():
            return None

        with open(self.data_path, 'rb') as f:
            f.seek(start_offset)
            while f.tell() < end_offset:
                header = f.read(8)
                if len(header) < 8:
                    break

                key_len, value_len = struct.unpack('>I I', header)
                record_key = f.read(key_len).decode('utf-8')

                if record_key == key:
                    return f.read(value_len).decode('utf-8')
                elif record_key > key:
                    return None
                else:
                    f.seek(value_len, 1)

        return None

    def range_scan(self, start_key, end_key):
        results = []
        if not self.data_path.exists():
            return results

        with open(self.data_path, 'rb') as f:
            while True:
                header = f.read(8)
                if len(header) < 8:
                    break

                key_len, value_len = struct.unpack('>I I', header)
                key = f.read(key_len).decode('utf-8')

                if key > end_key:
                    break

                value = f.read(value_len).decode('utf-8')

                if key >= start_key:
                    results.append((key, value))

        return results

class BloomFilter:
    def __init__(self, size=10000, num_hashes=3):
        self.size = size
        self.num_hashes = num_hashes
        self.bits = [False] * size

    def _hashes(self, key):
        h1 = hash(key)
        h2 = hash(key + '_salt')
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.size

    def add(self, key):
        for h in self._hashes(key):
            self.bits[h] = True

    def might_contain(self, key):
        return all(self.bits[h] for h in self._hashes(key))

    def rebuild(self, keys):
        self.bits = [False] * self.size
        for key in keys:
            self.add(key)


