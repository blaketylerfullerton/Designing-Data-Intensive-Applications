import os
import json
import hashlib
import threading
from pathlib import Path
from typing import Callable, Iterator, Tuple, Any

class Mapper:
    def __init__(self, map_func: Callable, num_partitions: int = 4, output_dir: str = 'map_output'):
        self.map_func = map_func
        self.num_partitions = num_partitions
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _partition(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.num_partitions

    def process_file(self, input_path: str, task_id: str) -> dict:
        partition_files = {}
        for i in range(self.num_partitions):
            path = self.output_dir / f'{task_id}_partition_{i}.json'
            partition_files[i] = open(path, 'w')

        record_count = 0
        with open(input_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                for key, value in self.map_func(line):
                    partition = self._partition(key)
                    partition_files[partition].write(json.dumps({'k': key, 'v': value}) + '\n')
                    record_count += 1

        for f in partition_files.values():
            f.close()

        return {
            'task_id': task_id,
            'input': input_path,
            'records': record_count,
            'partitions': [str(self.output_dir / f'{task_id}_partition_{i}.json') for i in range(self.num_partitions)]
        }

    def process_records(self, records: Iterator, task_id: str) -> dict:
        partition_files = {}
        for i in range(self.num_partitions):
            path = self.output_dir / f'{task_id}_partition_{i}.json'
            partition_files[i] = open(path, 'w')

        record_count = 0
        for record in records:
            for key, value in self.map_func(record):
                partition = self._partition(key)
                partition_files[partition].write(json.dumps({'k': key, 'v': value}) + '\n')
                record_count += 1

        for f in partition_files.values():
            f.close()

        return {
            'task_id': task_id,
            'records': record_count,
            'partitions': [str(self.output_dir / f'{task_id}_partition_{i}.json') for i in range(self.num_partitions)]
        }

class CombiningMapper(Mapper):
    def __init__(self, map_func: Callable, combine_func: Callable, num_partitions: int = 4, output_dir: str = 'map_output'):
        super().__init__(map_func, num_partitions, output_dir)
        self.combine_func = combine_func

    def process_file(self, input_path: str, task_id: str) -> dict:
        partition_buffers = {i: {} for i in range(self.num_partitions)}

        with open(input_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                for key, value in self.map_func(line):
                    partition = self._partition(key)
                    buffer = partition_buffers[partition]

                    if key in buffer:
                        buffer[key] = self.combine_func(key, [buffer[key], value])
                    else:
                        buffer[key] = value

        record_count = 0
        for i in range(self.num_partitions):
            path = self.output_dir / f'{task_id}_partition_{i}.json'
            with open(path, 'w') as f:
                for key, value in sorted(partition_buffers[i].items()):
                    f.write(json.dumps({'k': key, 'v': value}) + '\n')
                    record_count += 1

        return {
            'task_id': task_id,
            'input': input_path,
            'records': record_count,
            'partitions': [str(self.output_dir / f'{task_id}_partition_{i}.json') for i in range(self.num_partitions)]
        }

class ParallelMapper:
    def __init__(self, map_func: Callable, num_partitions: int = 4, num_workers: int = 4, output_dir: str = 'map_output'):
        self.map_func = map_func
        self.num_partitions = num_partitions
        self.num_workers = num_workers
        self.output_dir = Path(output_dir)

    def process_files(self, input_files: list) -> list:
        results = []
        threads = []
        lock = threading.Lock()

        def worker(files):
            mapper = Mapper(self.map_func, self.num_partitions, str(self.output_dir))
            for i, path in enumerate(files):
                task_id = f'map_{hash(path) % 10000:04d}'
                result = mapper.process_file(path, task_id)
                with lock:
                    results.append(result)

        chunk_size = (len(input_files) + self.num_workers - 1) // self.num_workers
        chunks = [input_files[i:i + chunk_size] for i in range(0, len(input_files), chunk_size)]

        for chunk in chunks:
            t = threading.Thread(target=worker, args=(chunk,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return results

def word_count_map(line: str) -> Iterator[Tuple[str, int]]:
    for word in line.lower().split():
        word = ''.join(c for c in word if c.isalnum())
        if word:
            yield (word, 1)

def inverted_index_map(line: str) -> Iterator[Tuple[str, str]]:
    parts = line.split('\t', 1)
    if len(parts) == 2:
        doc_id, content = parts
        for word in content.lower().split():
            word = ''.join(c for c in word if c.isalnum())
            if word:
                yield (word, doc_id)

def url_count_map(line: str) -> Iterator[Tuple[str, int]]:
    try:
        record = json.loads(line)
        url = record.get('url', '')
        if '://' in url:
            domain = url.split('://')[1].split('/')[0]
            yield (domain, 1)
    except:
        pass

if __name__ == '__main__':
    test_dir = Path('test_data')
    test_dir.mkdir(exist_ok=True)

    with open(test_dir / 'input.txt', 'w') as f:
        f.write('hello world\n')
        f.write('hello hadoop\n')
        f.write('hello mapreduce world\n')
        f.write('distributed systems\n')

    mapper = Mapper(word_count_map, num_partitions=2)
    result = mapper.process_file(str(test_dir / 'input.txt'), 'test_task')
    print(f'mapper result: {result}')

    for path in result['partitions']:
        print(f'\n{path}:')
        with open(path) as f:
            for line in f:
                print(f'  {line.strip()}')


