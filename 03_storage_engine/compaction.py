import threading
import time
from pathlib import Path
from log_store import LogSegment, LogStore

class Compactor:
    def __init__(self, log_store, interval=10.0):
        self.log_store = log_store
        self.interval = interval
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _run(self):
        while self.running:
            time.sleep(self.interval)
            self.compact()

    def compact(self):
        segments = self.log_store.get_segments_to_compact(threshold=2)
        if len(segments) < 2:
            return False

        merged_records = {}
        for segment in segments:
            for offset, key, value, deleted in segment.iterate():
                if deleted:
                    merged_records.pop(key, None)
                else:
                    merged_records[key] = value

        if not merged_records:
            return False

        new_segment_id = min(s.segment_id for s in segments)
        new_path = self.log_store.data_dir / f'segment_{new_segment_id:06d}_compacted.log'
        new_segment = LogSegment(new_path, new_segment_id)

        for key in sorted(merged_records.keys()):
            new_segment.append(key, merged_records[key])

        final_path = self.log_store.data_dir / f'segment_{new_segment_id:06d}.log'
        new_path.rename(final_path)
        new_segment.path = final_path

        self.log_store.replace_segments(segments, new_segment)
        return True

class LeveledCompactor:
    def __init__(self, log_store, level_size_ratio=10, max_levels=4):
        self.log_store = log_store
        self.level_size_ratio = level_size_ratio
        self.max_levels = max_levels
        self.levels = [[] for _ in range(max_levels)]
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _run(self):
        while self.running:
            time.sleep(5.0)
            self._check_levels()

    def _level_size_limit(self, level):
        return (self.level_size_ratio ** (level + 1)) * 1024 * 1024

    def _level_size(self, level):
        return sum(s.size for s in self.levels[level])

    def _check_levels(self):
        for level in range(self.max_levels - 1):
            if self._level_size(level) > self._level_size_limit(level):
                self._compact_level(level)

    def _compact_level(self, level):
        if not self.levels[level]:
            return

        segment = self.levels[level].pop(0)
        records = {}
        for offset, key, value, deleted in segment.iterate():
            if deleted:
                records.pop(key, None)
            else:
                records[key] = value

        if level + 1 < self.max_levels:
            overlapping = []
            for next_seg in self.levels[level + 1]:
                overlapping.append(next_seg)

            for next_seg in overlapping:
                for offset, key, value, deleted in next_seg.iterate():
                    if key not in records:
                        if deleted:
                            records.pop(key, None)
                        else:
                            records[key] = value

            for seg in overlapping:
                self.levels[level + 1].remove(seg)

        if records:
            new_id = int(time.time() * 1000)
            new_path = self.log_store.data_dir / f'level_{level+1}_seg_{new_id}.log'
            new_segment = LogSegment(new_path, new_id)

            for key in sorted(records.keys()):
                new_segment.append(key, records[key])

            self.levels[level + 1].append(new_segment)

