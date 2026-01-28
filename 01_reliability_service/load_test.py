import http.client
import json
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

class LoadGenerator:
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
        self.results = defaultdict(int)
        self.latencies = []
        self.lock = threading.Lock()

    def single_request(self):
        start = time.time()
        try:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=10)
            conn.request('GET', '/')
            response = conn.getresponse()
            response.read()
            conn.close()
            latency = (time.time() - start) * 1000
            with self.lock:
                self.results[response.status] += 1
                self.latencies.append(latency)
            return response.status
        except Exception as e:
            with self.lock:
                self.results['error'] += 1
            return None

    def configure_server(self, failure_rate=None, latency_ms=None):
        data = {}
        if failure_rate is not None:
            data['failure_rate'] = failure_rate
        if latency_ms is not None:
            data['latency_ms'] = latency_ms
        try:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=5)
            conn.request('POST', '/config', json.dumps(data), {'Content-Type': 'application/json'})
            conn.getresponse().read()
            conn.close()
        except:
            pass

    def run_load(self, requests_per_second, duration_seconds, max_workers=50):
        self.results.clear()
        self.latencies.clear()

        interval = 1.0 / requests_per_second
        end_time = time.time() + duration_seconds
        request_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while time.time() < end_time:
                executor.submit(self.single_request)
                request_count += 1
                time.sleep(interval)

        time.sleep(1)

        return self.get_stats()

    def get_stats(self):
        with self.lock:
            total = sum(v for k, v in self.results.items() if k != 'error')
            errors = self.results.get('error', 0)
            success = self.results.get(200, 0)
            failures = self.results.get(503, 0)

            if self.latencies:
                sorted_lat = sorted(self.latencies)
                p50 = sorted_lat[len(sorted_lat) // 2]
                p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
                p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
                avg = sum(sorted_lat) / len(sorted_lat)
            else:
                p50 = p95 = p99 = avg = 0

            return {
                'total_requests': total + errors,
                'success': success,
                'server_failures': failures,
                'connection_errors': errors,
                'success_rate': success / (total + errors) if (total + errors) > 0 else 0,
                'latency_avg_ms': round(avg, 2),
                'latency_p50_ms': round(p50, 2),
                'latency_p95_ms': round(p95, 2),
                'latency_p99_ms': round(p99, 2)
            }

def run_scenarios():
    gen = LoadGenerator()

    print('scenario 1: baseline (no failures)')
    gen.configure_server(failure_rate=0.0, latency_ms=0)
    stats = gen.run_load(requests_per_second=50, duration_seconds=5)
    print(f'  {stats}')

    print('\nscenario 2: 10% failure rate')
    gen.configure_server(failure_rate=0.1, latency_ms=0)
    stats = gen.run_load(requests_per_second=50, duration_seconds=5)
    print(f'  {stats}')

    print('\nscenario 3: 50% failure rate')
    gen.configure_server(failure_rate=0.5, latency_ms=0)
    stats = gen.run_load(requests_per_second=50, duration_seconds=5)
    print(f'  {stats}')

    print('\nscenario 4: added latency (100ms)')
    gen.configure_server(failure_rate=0.0, latency_ms=100)
    stats = gen.run_load(requests_per_second=20, duration_seconds=5)
    print(f'  {stats}')

    print('\nscenario 5: latency + failures')
    gen.configure_server(failure_rate=0.2, latency_ms=50)
    stats = gen.run_load(requests_per_second=30, duration_seconds=5)
    print(f'  {stats}')

    print('\nscenario 6: high load')
    gen.configure_server(failure_rate=0.0, latency_ms=10)
    stats = gen.run_load(requests_per_second=200, duration_seconds=5)
    print(f'  {stats}')

if __name__ == '__main__':
    run_scenarios()

