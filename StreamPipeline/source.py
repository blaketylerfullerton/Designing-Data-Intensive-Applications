import time
import json
import threading
import random
from dataclasses import dataclass
from typing import Iterator, Callable, Optional
from queue import Queue

@dataclass
class Event:
    key: str
    value: any
    event_time: float
    processing_time: float = None

    def __post_init__(self):
        if self.processing_time is None:
            self.processing_time = time.time()

    def to_dict(self):
        return {
            'key': self.key,
            'value': self.value,
            'event_time': self.event_time,
            'processing_time': self.processing_time
        }

    @classmethod
    def from_dict(cls, d):
        return cls(d['key'], d['value'], d['event_time'], d.get('processing_time'))

class EventSource:
    def __init__(self):
        self.running = False
        self.subscribers = []

    def subscribe(self, callback: Callable[[Event], None]):
        self.subscribers.append(callback)

    def emit(self, event: Event):
        for callback in self.subscribers:
            callback(event)

    def start(self):
        self.running = True

    def stop(self):
        self.running = False

class GeneratorSource(EventSource):
    def __init__(self, generator_func: Callable, interval: float = 0.1):
        super().__init__()
        self.generator_func = generator_func
        self.interval = interval
        self.thread = None

    def start(self):
        super().start()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        for event in self.generator_func():
            if not self.running:
                break
            self.emit(event)
            time.sleep(self.interval)

class FileSource(EventSource):
    def __init__(self, path: str, parse_func: Callable = None, replay_speed: float = 1.0):
        super().__init__()
        self.path = path
        self.parse_func = parse_func or self._default_parse
        self.replay_speed = replay_speed
        self.thread = None

    def _default_parse(self, line: str) -> Optional[Event]:
        try:
            data = json.loads(line)
            return Event.from_dict(data)
        except:
            return None

    def start(self):
        super().start()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        last_event_time = None

        with open(self.path) as f:
            for line in f:
                if not self.running:
                    break

                event = self.parse_func(line.strip())
                if event is None:
                    continue

                if last_event_time is not None:
                    delay = (event.event_time - last_event_time) / self.replay_speed
                    if delay > 0:
                        time.sleep(delay)

                last_event_time = event.event_time
                event.processing_time = time.time()
                self.emit(event)

class SocketSource(EventSource):
    def __init__(self, host: str, port: int, parse_func: Callable = None):
        super().__init__()
        self.host = host
        self.port = port
        self.parse_func = parse_func or self._default_parse
        self.thread = None
        self.socket = None

    def _default_parse(self, data: bytes) -> Optional[Event]:
        try:
            d = json.loads(data.decode())
            return Event.from_dict(d)
        except:
            return None

    def start(self):
        import socket
        super().start()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        super().stop()
        if self.socket:
            self.socket.close()

    def _run(self):
        buffer = b''
        while self.running:
            try:
                data = self.socket.recv(4096)
                if not data:
                    break

                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    event = self.parse_func(line)
                    if event:
                        self.emit(event)
            except:
                break

class QueueSource(EventSource):
    def __init__(self):
        super().__init__()
        self.queue = Queue()
        self.thread = None

    def put(self, event: Event):
        self.queue.put(event)

    def start(self):
        super().start()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        while self.running:
            try:
                event = self.queue.get(timeout=0.1)
                self.emit(event)
            except:
                continue

def click_stream_generator() -> Iterator[Event]:
    users = ['user_1', 'user_2', 'user_3', 'user_4', 'user_5']
    pages = ['/home', '/products', '/cart', '/checkout', '/profile']

    while True:
        user = random.choice(users)
        page = random.choice(pages)
        event_time = time.time() - random.uniform(0, 5)

        yield Event(
            key=user,
            value={'page': page, 'duration': random.randint(1, 60)},
            event_time=event_time
        )

def sensor_data_generator(num_sensors: int = 10) -> Iterator[Event]:
    while True:
        sensor_id = f'sensor_{random.randint(0, num_sensors - 1)}'
        event_time = time.time() - random.uniform(0, 2)

        yield Event(
            key=sensor_id,
            value={
                'temperature': round(20 + random.gauss(0, 5), 2),
                'humidity': round(50 + random.gauss(0, 10), 2)
            },
            event_time=event_time
        )

def transaction_generator() -> Iterator[Event]:
    accounts = [f'account_{i}' for i in range(100)]

    while True:
        account = random.choice(accounts)
        event_time = time.time() - random.uniform(0, 3)

        yield Event(
            key=account,
            value={
                'type': random.choice(['deposit', 'withdrawal', 'transfer']),
                'amount': round(random.uniform(10, 1000), 2)
            },
            event_time=event_time
        )

if __name__ == '__main__':
    source = GeneratorSource(click_stream_generator, interval=0.5)

    def print_event(event):
        print(f'{event.key}: {event.value} (event_time={event.event_time:.2f})')

    source.subscribe(print_event)
    source.start()

    time.sleep(5)
    source.stop()


