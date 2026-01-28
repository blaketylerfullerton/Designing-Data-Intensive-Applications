import http.client
import json
import time
import threading
from enum import Enum

class CircuitState(Enum):
    CLOSED = 'closed'
    OPEN = 'open'
    HALF_OPEN = 'half_open'

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30.0, half_open_max=3):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max = half_open_max
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.half_open_successes = 0
        self.lock = threading.Lock()

    def can_execute(self):
        with self.lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_successes = 0
                    return True
                return False
            return True

    def record_success(self):
        with self.lock:
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_max:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
            else:
                self.failure_count = 0

    def record_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
            elif self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN

    def get_state(self):
        with self.lock:
            return self.state

class RateLimiter:
    def __init__(self, max_requests, window_seconds):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = []
        self.lock = threading.Lock()

    def allow(self):
        with self.lock:
            now = time.time()
            cutoff = now - self.window_seconds
            self.requests = [t for t in self.requests if t > cutoff]
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            return False

class RetryPolicy:
    def __init__(self, max_retries=3, base_delay=0.1, max_delay=10.0, exponential=True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential = exponential

    def get_delay(self, attempt):
        if self.exponential:
            delay = self.base_delay * (2 ** attempt)
        else:
            delay = self.base_delay
        return min(delay, self.max_delay)

class ReliableClient:
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
        self.circuit = CircuitBreaker()
        self.limiter = RateLimiter(max_requests=100, window_seconds=1.0)
        self.retry_policy = RetryPolicy()

    def get(self, path='/'):
        if not self.limiter.allow():
            return {'error': 'rate_limited', 'success': False}

        if not self.circuit.can_execute():
            return {'error': 'circuit_open', 'success': False, 'state': self.circuit.get_state().value}

        for attempt in range(self.retry_policy.max_retries + 1):
            try:
                conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
                conn.request('GET', path)
                response = conn.getresponse()
                body = response.read().decode()
                conn.close()

                if response.status == 200:
                    self.circuit.record_success()
                    data = json.loads(body)
                    data['success'] = True
                    data['attempts'] = attempt + 1
                    return data
                else:
                    self.circuit.record_failure()
                    if attempt < self.retry_policy.max_retries:
                        time.sleep(self.retry_policy.get_delay(attempt))
                    continue

            except Exception as e:
                self.circuit.record_failure()
                if attempt < self.retry_policy.max_retries:
                    time.sleep(self.retry_policy.get_delay(attempt))
                continue

        return {'error': 'max_retries_exceeded', 'success': False, 'attempts': self.retry_policy.max_retries + 1}

    def configure_server(self, failure_rate=None, latency_ms=None):
        data = {}
        if failure_rate is not None:
            data['failure_rate'] = failure_rate
        if latency_ms is not None:
            data['latency_ms'] = latency_ms

        try:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
            body = json.dumps(data)
            conn.request('POST', '/config', body, {'Content-Type': 'application/json'})
            response = conn.getresponse()
            result = json.loads(response.read().decode())
            conn.close()
            return result
        except Exception as e:
            return {'error': str(e)}

    def get_stats(self):
        return {
            'circuit_state': self.circuit.get_state().value,
            'circuit_failures': self.circuit.failure_count
        }

if __name__ == '__main__':
    client = ReliableClient()

    print('normal operation:')
    for i in range(5):
        result = client.get('/')
        print(f'  request {i+1}: {result}')

    print('\ninjecting 50% failures:')
    client.configure_server(failure_rate=0.5)
    for i in range(10):
        result = client.get('/')
        print(f'  request {i+1}: {result}')
        print(f'  client stats: {client.get_stats()}')

    print('\ninjecting 100% failures to trigger circuit breaker:')
    client.configure_server(failure_rate=1.0)
    for i in range(10):
        result = client.get('/')
        print(f'  request {i+1}: {result}')
        print(f'  client stats: {client.get_stats()}')


