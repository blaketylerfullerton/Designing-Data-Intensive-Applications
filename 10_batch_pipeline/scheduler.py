import threading
import time
import uuid
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable
from pathlib import Path

class TaskState(Enum):
    PENDING = 'pending'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'

@dataclass
class Task:
    task_id: str
    task_type: str
    input_files: List[str]
    output_dir: str
    state: TaskState = TaskState.PENDING
    worker_id: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    result: Optional[dict] = None
    error: Optional[str] = None
    attempts: int = 0

class JobScheduler:
    def __init__(self, max_retries: int = 3, task_timeout: float = 300.0):
        self.tasks: Dict[str, Task] = {}
        self.pending_queue: List[str] = []
        self.max_retries = max_retries
        self.task_timeout = task_timeout
        self.lock = threading.Lock()
        self.workers: Dict[str, dict] = {}
        self.running = False

    def add_task(self, task: Task):
        with self.lock:
            self.tasks[task.task_id] = task
            self.pending_queue.append(task.task_id)

    def get_next_task(self, worker_id: str) -> Optional[Task]:
        with self.lock:
            if not self.pending_queue:
                return None

            task_id = self.pending_queue.pop(0)
            task = self.tasks[task_id]
            task.state = TaskState.RUNNING
            task.worker_id = worker_id
            task.start_time = time.time()
            task.attempts += 1

            return task

    def complete_task(self, task_id: str, result: dict):
        with self.lock:
            if task_id not in self.tasks:
                return

            task = self.tasks[task_id]
            task.state = TaskState.COMPLETED
            task.end_time = time.time()
            task.result = result

    def fail_task(self, task_id: str, error: str):
        with self.lock:
            if task_id not in self.tasks:
                return

            task = self.tasks[task_id]

            if task.attempts < self.max_retries:
                task.state = TaskState.PENDING
                task.worker_id = None
                task.error = error
                self.pending_queue.append(task_id)
            else:
                task.state = TaskState.FAILED
                task.end_time = time.time()
                task.error = error

    def check_timeouts(self):
        with self.lock:
            now = time.time()
            for task_id, task in self.tasks.items():
                if task.state == TaskState.RUNNING:
                    if task.start_time and now - task.start_time > self.task_timeout:
                        if task.attempts < self.max_retries:
                            task.state = TaskState.PENDING
                            task.worker_id = None
                            task.error = 'timeout'
                            self.pending_queue.append(task_id)
                        else:
                            task.state = TaskState.FAILED
                            task.error = 'timeout after max retries'

    def get_status(self) -> dict:
        with self.lock:
            counts = {s: 0 for s in TaskState}
            for task in self.tasks.values():
                counts[task.state] += 1

            return {
                'total': len(self.tasks),
                'pending': counts[TaskState.PENDING],
                'running': counts[TaskState.RUNNING],
                'completed': counts[TaskState.COMPLETED],
                'failed': counts[TaskState.FAILED]
            }

    def all_complete(self) -> bool:
        with self.lock:
            for task in self.tasks.values():
                if task.state not in (TaskState.COMPLETED, TaskState.FAILED):
                    return False
            return True

    def any_failed(self) -> bool:
        with self.lock:
            return any(t.state == TaskState.FAILED for t in self.tasks.values())

class Worker:
    def __init__(self, worker_id: str, scheduler: JobScheduler, map_func: Callable = None, reduce_func: Callable = None):
        self.worker_id = worker_id
        self.scheduler = scheduler
        self.map_func = map_func
        self.reduce_func = reduce_func
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
        from mapper import Mapper
        from reducer import Reducer

        while self.running:
            task = self.scheduler.get_next_task(self.worker_id)

            if task is None:
                time.sleep(0.1)
                continue

            try:
                if task.task_type == 'map':
                    mapper = Mapper(self.map_func, output_dir=task.output_dir)
                    result = mapper.process_file(task.input_files[0], task.task_id)
                    self.scheduler.complete_task(task.task_id, result)

                elif task.task_type == 'reduce':
                    reducer = Reducer(self.reduce_func, output_dir=task.output_dir)
                    partition_id = int(task.task_id.split('_')[-1])
                    result = reducer.process_partition(task.input_files, partition_id)
                    self.scheduler.complete_task(task.task_id, result)

            except Exception as e:
                self.scheduler.fail_task(task.task_id, str(e))

class WorkerPool:
    def __init__(self, num_workers: int, scheduler: JobScheduler, map_func: Callable = None, reduce_func: Callable = None):
        self.workers = []
        for i in range(num_workers):
            worker = Worker(f'worker_{i}', scheduler, map_func, reduce_func)
            self.workers.append(worker)

    def start(self):
        for worker in self.workers:
            worker.start()

    def stop(self):
        for worker in self.workers:
            worker.stop()

class SpeculativeExecution:
    def __init__(self, scheduler: JobScheduler, threshold: float = 0.8, slow_threshold: float = 1.5):
        self.scheduler = scheduler
        self.threshold = threshold
        self.slow_threshold = slow_threshold
        self.speculative_tasks: Dict[str, str] = {}

    def check_for_slow_tasks(self):
        with self.scheduler.lock:
            completed_times = []
            for task in self.scheduler.tasks.values():
                if task.state == TaskState.COMPLETED and task.start_time and task.end_time:
                    completed_times.append(task.end_time - task.start_time)

            if not completed_times:
                return []

            avg_time = sum(completed_times) / len(completed_times)
            slow_tasks = []

            now = time.time()
            for task_id, task in self.scheduler.tasks.items():
                if task.state == TaskState.RUNNING and task.start_time:
                    running_time = now - task.start_time
                    if running_time > avg_time * self.slow_threshold:
                        if task_id not in self.speculative_tasks:
                            slow_tasks.append(task_id)

            return slow_tasks

    def launch_speculative_task(self, original_task_id: str) -> Optional[str]:
        with self.scheduler.lock:
            if original_task_id not in self.scheduler.tasks:
                return None

            original = self.scheduler.tasks[original_task_id]
            spec_id = f'{original_task_id}_spec_{uuid.uuid4().hex[:8]}'

            spec_task = Task(
                task_id=spec_id,
                task_type=original.task_type,
                input_files=original.input_files,
                output_dir=original.output_dir
            )

            self.scheduler.tasks[spec_id] = spec_task
            self.scheduler.pending_queue.append(spec_id)
            self.speculative_tasks[original_task_id] = spec_id

            return spec_id

