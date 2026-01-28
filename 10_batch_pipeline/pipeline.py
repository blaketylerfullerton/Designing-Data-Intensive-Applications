import os
import json
import shutil
import time
from pathlib import Path
from typing import Callable, List
from dataclasses import dataclass

from mapper import Mapper, word_count_map, ParallelMapper
from reducer import Reducer, sum_reduce
from scheduler import JobScheduler, WorkerPool, Task

@dataclass
class JobConfig:
    name: str
    input_paths: List[str]
    output_dir: str
    map_func: Callable
    reduce_func: Callable
    num_mappers: int = 4
    num_reducers: int = 2
    num_partitions: int = 4

class MapReduceJob:
    def __init__(self, config: JobConfig):
        self.config = config
        self.work_dir = Path(config.output_dir) / 'work'
        self.map_output = self.work_dir / 'map'
        self.reduce_output = Path(config.output_dir) / 'output'

        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.map_output.mkdir(parents=True, exist_ok=True)
        self.reduce_output.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict:
        start_time = time.time()

        map_results = self._run_map_phase()

        partition_files = self._shuffle(map_results)

        reduce_results = self._run_reduce_phase(partition_files)

        end_time = time.time()

        return {
            'job_name': self.config.name,
            'duration': end_time - start_time,
            'map_tasks': len(map_results),
            'reduce_tasks': len(reduce_results),
            'output_dir': str(self.reduce_output)
        }

    def _run_map_phase(self) -> List[dict]:
        results = []

        for i, input_path in enumerate(self.config.input_paths):
            mapper = Mapper(
                self.config.map_func,
                num_partitions=self.config.num_partitions,
                output_dir=str(self.map_output)
            )
            result = mapper.process_file(input_path, f'map_{i:04d}')
            results.append(result)

        return results

    def _shuffle(self, map_results: List[dict]) -> dict:
        partition_files = {i: [] for i in range(self.config.num_partitions)}

        for result in map_results:
            for i, path in enumerate(result['partitions']):
                partition_files[i].append(path)

        return partition_files

    def _run_reduce_phase(self, partition_files: dict) -> List[dict]:
        results = []

        for partition_id, files in partition_files.items():
            reducer = Reducer(
                self.config.reduce_func,
                output_dir=str(self.reduce_output)
            )
            result = reducer.process_partition(files, partition_id)
            results.append(result)

        return results

class DistributedMapReduceJob:
    def __init__(self, config: JobConfig, num_workers: int = 4):
        self.config = config
        self.num_workers = num_workers
        self.work_dir = Path(config.output_dir) / 'work'
        self.map_output = self.work_dir / 'map'
        self.reduce_output = Path(config.output_dir) / 'output'

        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.map_output.mkdir(parents=True, exist_ok=True)
        self.reduce_output.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict:
        start_time = time.time()

        map_scheduler = JobScheduler()
        for i, input_path in enumerate(self.config.input_paths):
            task = Task(
                task_id=f'map_{i:04d}',
                task_type='map',
                input_files=[input_path],
                output_dir=str(self.map_output)
            )
            map_scheduler.add_task(task)

        map_pool = WorkerPool(
            self.num_workers,
            map_scheduler,
            map_func=self.config.map_func
        )
        map_pool.start()

        while not map_scheduler.all_complete():
            time.sleep(0.1)

        map_pool.stop()

        if map_scheduler.any_failed():
            return {'error': 'map phase failed', 'status': map_scheduler.get_status()}

        partition_files = {i: [] for i in range(self.config.num_partitions)}
        for task in map_scheduler.tasks.values():
            if task.result:
                for i, path in enumerate(task.result['partitions']):
                    partition_files[i].append(path)

        reduce_scheduler = JobScheduler()
        for partition_id, files in partition_files.items():
            task = Task(
                task_id=f'reduce_{partition_id:04d}',
                task_type='reduce',
                input_files=files,
                output_dir=str(self.reduce_output)
            )
            reduce_scheduler.add_task(task)

        reduce_pool = WorkerPool(
            self.num_workers,
            reduce_scheduler,
            reduce_func=self.config.reduce_func
        )
        reduce_pool.start()

        while not reduce_scheduler.all_complete():
            time.sleep(0.1)

        reduce_pool.stop()

        if reduce_scheduler.any_failed():
            return {'error': 'reduce phase failed', 'status': reduce_scheduler.get_status()}

        end_time = time.time()

        return {
            'job_name': self.config.name,
            'duration': end_time - start_time,
            'map_tasks': len(map_scheduler.tasks),
            'reduce_tasks': len(reduce_scheduler.tasks),
            'output_dir': str(self.reduce_output)
        }

class ChainedJob:
    def __init__(self, jobs: List[JobConfig]):
        self.jobs = jobs

    def run(self) -> List[dict]:
        results = []
        prev_output = None

        for i, config in enumerate(self.jobs):
            if prev_output:
                output_files = list(Path(prev_output).glob('part_*.json'))
                config.input_paths = [str(f) for f in output_files]

            job = MapReduceJob(config)
            result = job.run()
            results.append(result)
            prev_output = result['output_dir']

        return results

def run_word_count():
    test_dir = Path('word_count_test')
    test_dir.mkdir(exist_ok=True)

    with open(test_dir / 'input1.txt', 'w') as f:
        f.write('hello world\n')
        f.write('hello mapreduce\n')
        f.write('distributed computing is fun\n')

    with open(test_dir / 'input2.txt', 'w') as f:
        f.write('hello hadoop spark\n')
        f.write('big data processing\n')
        f.write('mapreduce is powerful\n')

    config = JobConfig(
        name='word_count',
        input_paths=[str(test_dir / 'input1.txt'), str(test_dir / 'input2.txt')],
        output_dir=str(test_dir / 'output'),
        map_func=word_count_map,
        reduce_func=sum_reduce,
        num_partitions=2
    )

    job = MapReduceJob(config)
    result = job.run()

    print(f'job result: {result}')
    print('\noutput:')
    for path in sorted(Path(result['output_dir']).glob('part_*.json')):
        print(f'\n{path.name}:')
        with open(path) as f:
            for line in f:
                record = json.loads(line)
                print(f'  {record["k"]}: {record["v"]}')

if __name__ == '__main__':
    run_word_count()


