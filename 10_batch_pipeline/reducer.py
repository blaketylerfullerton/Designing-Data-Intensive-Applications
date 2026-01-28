import os
import json
import heapq
from pathlib import Path
from typing import Callable, Iterator, Tuple, Any, List

class Reducer:
    def __init__(self, reduce_func: Callable, output_dir: str = 'reduce_output'):
        self.reduce_func = reduce_func
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_partition(self, partition_files: List[str], partition_id: int) -> dict:
        output_path = self.output_dir / f'part_{partition_id:04d}.json'

        sorted_records = self._merge_sort(partition_files)

        record_count = 0
        with open(output_path, 'w') as out:
            current_key = None
            current_values = []

            for key, value in sorted_records:
                if key != current_key:
                    if current_key is not None:
                        result = self.reduce_func(current_key, current_values)
                        out.write(json.dumps({'k': current_key, 'v': result}) + '\n')
                        record_count += 1

                    current_key = key
                    current_values = [value]
                else:
                    current_values.append(value)

            if current_key is not None:
                result = self.reduce_func(current_key, current_values)
                out.write(json.dumps({'k': current_key, 'v': result}) + '\n')
                record_count += 1

        return {
            'partition_id': partition_id,
            'output': str(output_path),
            'records': record_count
        }

    def _merge_sort(self, partition_files: List[str]) -> Iterator[Tuple[str, Any]]:
        file_handles = []
        heap = []

        for i, path in enumerate(partition_files):
            if os.path.exists(path):
                f = open(path)
                file_handles.append(f)
                line = f.readline()
                if line:
                    record = json.loads(line)
                    heapq.heappush(heap, (record['k'], record['v'], i))

        while heap:
            key, value, file_idx = heapq.heappop(heap)
            yield (key, value)

            line = file_handles[file_idx].readline()
            if line:
                record = json.loads(line)
                heapq.heappush(heap, (record['k'], record['v'], file_idx))

        for f in file_handles:
            f.close()

class SortingReducer(Reducer):
    def __init__(self, reduce_func: Callable, sort_key: Callable = None, output_dir: str = 'reduce_output'):
        super().__init__(reduce_func, output_dir)
        self.sort_key = sort_key or (lambda x: x)

    def _merge_sort(self, partition_files: List[str]) -> Iterator[Tuple[str, Any]]:
        all_records = []

        for path in partition_files:
            if os.path.exists(path):
                with open(path) as f:
                    for line in f:
                        record = json.loads(line)
                        all_records.append((record['k'], record['v']))

        all_records.sort(key=lambda x: self.sort_key(x[0]))

        for record in all_records:
            yield record

class SecondarySort:
    def __init__(self, key_func: Callable, sort_func: Callable):
        self.key_func = key_func
        self.sort_func = sort_func

    def process(self, records: Iterator[Tuple[str, Any]]) -> Iterator[Tuple[str, List]]:
        groups = {}

        for key, value in records:
            group_key = self.key_func(key)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append((key, value))

        for group_key in sorted(groups.keys()):
            values = groups[group_key]
            values.sort(key=lambda x: self.sort_func(x[0]))
            yield (group_key, [v for k, v in values])

def sum_reduce(key: str, values: List[int]) -> int:
    return sum(values)

def list_reduce(key: str, values: List[Any]) -> List:
    return list(set(values))

def max_reduce(key: str, values: List[Any]) -> Any:
    return max(values)

def min_reduce(key: str, values: List[Any]) -> Any:
    return min(values)

def avg_reduce(key: str, values: List[float]) -> float:
    return sum(values) / len(values) if values else 0

def count_reduce(key: str, values: List[Any]) -> int:
    return len(values)

def top_n_reduce(n: int):
    def reducer(key: str, values: List[Any]) -> List:
        return sorted(values, reverse=True)[:n]
    return reducer

class CombinedReducer:
    def __init__(self, reducers: dict, output_dir: str = 'reduce_output'):
        self.reducers = reducers
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process(self, partition_files: List[str], partition_id: int) -> dict:
        results = {name: {} for name in self.reducers}

        for path in partition_files:
            if os.path.exists(path):
                with open(path) as f:
                    for line in f:
                        record = json.loads(line)
                        key, value = record['k'], record['v']

                        for name, reduce_func in self.reducers.items():
                            if key not in results[name]:
                                results[name][key] = []
                            results[name][key].append(value)

        outputs = {}
        for name, reduce_func in self.reducers.items():
            output_path = self.output_dir / f'{name}_part_{partition_id:04d}.json'
            with open(output_path, 'w') as out:
                for key in sorted(results[name].keys()):
                    values = results[name][key]
                    result = reduce_func(key, values)
                    out.write(json.dumps({'k': key, 'v': result}) + '\n')
            outputs[name] = str(output_path)

        return {'partition_id': partition_id, 'outputs': outputs}

if __name__ == '__main__':
    test_dir = Path('test_reduce')
    test_dir.mkdir(exist_ok=True)

    with open(test_dir / 'map_0.json', 'w') as f:
        f.write(json.dumps({'k': 'apple', 'v': 1}) + '\n')
        f.write(json.dumps({'k': 'apple', 'v': 1}) + '\n')
        f.write(json.dumps({'k': 'banana', 'v': 1}) + '\n')

    with open(test_dir / 'map_1.json', 'w') as f:
        f.write(json.dumps({'k': 'apple', 'v': 1}) + '\n')
        f.write(json.dumps({'k': 'cherry', 'v': 1}) + '\n')

    reducer = Reducer(sum_reduce)
    result = reducer.process_partition(
        [str(test_dir / 'map_0.json'), str(test_dir / 'map_1.json')],
        0
    )
    print(f'reducer result: {result}')

    print(f'\noutput:')
    with open(result['output']) as f:
        for line in f:
            print(f'  {line.strip()}')

