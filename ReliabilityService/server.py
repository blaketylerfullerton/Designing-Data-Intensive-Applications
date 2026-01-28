import http.server
import json
import random
import threading
import time
from http import HTTPStatus

class FailureConfig:
    def __init__(self):
        self.failure_rate = 0.0
        self.latency_ms = 0
        self.lock = threading.Lock()

    def set(self, failure_rate=None, latency_ms=None):
        with self.lock:
            if failure_rate is not None:
                self.failure_rate = failure_rate
            if latency_ms is not None:
                self.latency_ms = latency_ms

    def get(self):
        with self.lock:
            return self.failure_rate, self.latency_ms

config = FailureConfig()
request_count = 0
request_lock = threading.Lock()

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        global request_count
        with request_lock:
            request_count += 1
            current = request_count

        if self.path == '/health':
            self._send_json({'status': 'ok', 'requests': current})
            return

        if self.path == '/stats':
            failure_rate, latency = config.get()
            self._send_json({
                'requests': current,
                'failure_rate': failure_rate,
                'latency_ms': latency
            })
            return

        failure_rate, latency_ms = config.get()

        if latency_ms > 0:
            time.sleep(latency_ms / 1000.0)

        if random.random() < failure_rate:
            self._send_error(HTTPStatus.SERVICE_UNAVAILABLE, 'simulated failure')
            return

        self._send_json({'message': 'success', 'request_id': current})

    def do_POST(self):
        if self.path == '/config':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
                config.set(
                    failure_rate=data.get('failure_rate'),
                    latency_ms=data.get('latency_ms')
                )
                self._send_json({'status': 'configured'})
            except json.JSONDecodeError:
                self._send_error(HTTPStatus.BAD_REQUEST, 'invalid json')
            return

        self._send_error(HTTPStatus.NOT_FOUND, 'not found')

    def _send_json(self, data):
        body = json.dumps(data).encode()
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status, message):
        body = json.dumps({'error': message}).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

def run(port=8080):
    server = http.server.HTTPServer(('', port), RequestHandler)
    print(f'server listening on port {port}')
    server.serve_forever()

if __name__ == '__main__':
    run()


