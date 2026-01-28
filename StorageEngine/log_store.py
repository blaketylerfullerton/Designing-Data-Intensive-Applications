import os
import struct
import threading
import time
from pathlib import Path

SEGMENT_MAX_SIZE = 1024 * 1024

class LogSegment:
    def __init__(self, path, segment_id):
        self.path = Path(path)
        self.segment_id = segment_id
        self.size = 0
        self.lock = threading.Lock()

        if self.path.exists():
            self.size = self.path.stat().st_size

    def append(self, key, value):
        with self.lock:
            key_bytes = key.encode('utf-8')
            value_bytes = value.encode('utf-8') if value is not None else b''
            deleted = 1 if value is None else 0

            header = struct.pack('>I I B', len(key_bytes), len(value_bytes), deleted)
            record = header + key_bytes + value_bytes

            offset = self.size
            with open(self.path, 'ab') as f:
                f.write(record)

            self.size += len(record)
            return offset

    def read_at(self, offset):
        with open(self.path, 'rb') as f:
            f.seek(offset)
            header = f.read(9)
            if len(header) < 9:
                return None, None, False

            key_len, value_len, deleted = struct.unpack('>I I B', header)
            key = f.read(key_len).decode('utf-8')

            if deleted:
                return key, None, True

            value = f.read(value_len).decode('utf-8')
            return key, value, False

    def iterate(self):
        if not self.path.exists():
            return

        with open(self.path, 'rb') as f:
            offset = 0
            while True:
                header = f.read(9)
                if len(header) < 9:
                    break

                key_len, value_len, deleted = struct.unpack('>I I B', header)
                key = f.read(key_len).decode('utf-8')

                if deleted:
                    yield offset, key, None, True
                else:
                    value = f.read(value_len).decode('utf-8')
                    yield offset, key, value, False

                offset = f.tell()

    def is_full(self):
        return self.size >= SEGMENT_MAX_SIZE

class LogStore:
    def __init__(self, data_dir='log_data'):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.segments = []
        self.active_segment = None
        self.lock = threading.Lock()
        self._load_segments()

    def _load_segments(self):
        segment_files = sorted(self.data_dir.glob('segment_*.log'))
        for path in segment_files:
            segment_id = int(path.stem.split('_')[1])
            segment = LogSegment(path, segment_id)
            self.segments.append(segment)

        if not self.segments:
            self._create_new_segment()
        else:
            self.active_segment = self.segments[-1]

    def _create_new_segment(self):
        segment_id = len(self.segments)
        path = self.data_dir / f'segment_{segment_id:06d}.log'
        segment = LogSegment(path, segment_id)
        self.segments.append(segment)
        self.active_segment = segment
        return segment

    def put(self, key, value):
        with self.lock:
            if self.active_segment.is_full():
                self._create_new_segment()
            offset = self.active_segment.append(key, value)
            return (self.active_segment.segment_id, offset)

    def delete(self, key):
        return self.put(key, None)

    def get_all_records(self):
        records = {}
        for segment in self.segments:
            for offset, key, value, deleted in segment.iterate():
                if deleted:
                    records.pop(key, None)
                else:
                    records[key] = (segment.segment_id, offset, value)
        return records

    def get_segment(self, segment_id):
        for segment in self.segments:
            if segment.segment_id == segment_id:
                return segment
        return None

    def get_segments_to_compact(self, threshold=2):
        return [s for s in self.segments[:-1] if s != self.active_segment][:threshold]

    def replace_segments(self, old_segments, new_segment):
        with self.lock:
            old_ids = {s.segment_id for s in old_segments}
            self.segments = [s for s in self.segments if s.segment_id not in old_ids]

            insert_pos = 0
            for i, s in enumerate(self.segments):
                if s.segment_id > new_segment.segment_id:
                    insert_pos = i
                    break
                insert_pos = i + 1

            self.segments.insert(insert_pos, new_segment)

            for old_segment in old_segments:
                if old_segment.path.exists():
                    old_segment.path.unlink()


