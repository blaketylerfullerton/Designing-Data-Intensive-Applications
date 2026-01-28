import http.server
import json
from http import HTTPStatus
from urllib.parse import urlparse, parse_qs

class StoreInterface:
    def create_user(self, user_id, data): raise NotImplementedError
    def get_user(self, user_id): raise NotImplementedError
    def update_user(self, user_id, data): raise NotImplementedError
    def delete_user(self, user_id): raise NotImplementedError
    def add_relationship(self, from_id, to_id, rel_type): raise NotImplementedError
    def get_relationships(self, user_id, rel_type=None): raise NotImplementedError
    def query_users(self, filters): raise NotImplementedError

current_store = None

def set_store(store):
    global current_store
    current_store = store

class APIHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path_parts = parsed.path.strip('/').split('/')

        if len(path_parts) >= 2 and path_parts[0] == 'users':
            user_id = path_parts[1]
            if len(path_parts) == 2:
                user = current_store.get_user(user_id)
                if user:
                    self._send_json(user)
                else:
                    self._send_error(HTTPStatus.NOT_FOUND, 'user not found')
            elif len(path_parts) == 3 and path_parts[2] == 'relationships':
                qs = parse_qs(parsed.query)
                rel_type = qs.get('type', [None])[0]
                rels = current_store.get_relationships(user_id, rel_type)
                self._send_json({'relationships': rels})
            else:
                self._send_error(HTTPStatus.NOT_FOUND, 'not found')
        elif path_parts[0] == 'users' and len(path_parts) == 1:
            qs = parse_qs(parsed.query)
            filters = {k: v[0] for k, v in qs.items()}
            users = current_store.query_users(filters)
            self._send_json({'users': users})
        else:
            self._send_error(HTTPStatus.NOT_FOUND, 'not found')

    def do_POST(self):
        parsed = urlparse(self.path)
        path_parts = parsed.path.strip('/').split('/')
        body = self._read_body()

        if path_parts[0] == 'users' and len(path_parts) == 1:
            if 'id' not in body:
                self._send_error(HTTPStatus.BAD_REQUEST, 'id required')
                return
            current_store.create_user(body['id'], body)
            self._send_json({'status': 'created', 'id': body['id']}, HTTPStatus.CREATED)

        elif len(path_parts) == 3 and path_parts[0] == 'users' and path_parts[2] == 'relationships':
            user_id = path_parts[1]
            if 'to' not in body or 'type' not in body:
                self._send_error(HTTPStatus.BAD_REQUEST, 'to and type required')
                return
            current_store.add_relationship(user_id, body['to'], body['type'])
            self._send_json({'status': 'relationship created'}, HTTPStatus.CREATED)
        else:
            self._send_error(HTTPStatus.NOT_FOUND, 'not found')

    def do_PUT(self):
        parsed = urlparse(self.path)
        path_parts = parsed.path.strip('/').split('/')
        body = self._read_body()

        if len(path_parts) == 2 and path_parts[0] == 'users':
            user_id = path_parts[1]
            current_store.update_user(user_id, body)
            self._send_json({'status': 'updated'})
        else:
            self._send_error(HTTPStatus.NOT_FOUND, 'not found')

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path_parts = parsed.path.strip('/').split('/')

        if len(path_parts) == 2 and path_parts[0] == 'users':
            user_id = path_parts[1]
            current_store.delete_user(user_id)
            self._send_json({'status': 'deleted'})
        else:
            self._send_error(HTTPStatus.NOT_FOUND, 'not found')

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def _send_json(self, data, status=HTTPStatus.OK):
        body = json.dumps(data).encode()
        self.send_response(status)
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

def run(store, port=8081):
    set_store(store)
    server = http.server.HTTPServer(('', port), APIHandler)
    print(f'api server on port {port} using {store.__class__.__name__}')
    server.serve_forever()

if __name__ == '__main__':
    import sys
    from relational_store import RelationalStore
    from document_store import DocumentStore
    from graph_store import GraphStore

    stores = {
        'relational': RelationalStore,
        'document': DocumentStore,
        'graph': GraphStore
    }

    store_type = sys.argv[1] if len(sys.argv) > 1 else 'relational'
    if store_type not in stores:
        print(f'unknown store: {store_type}')
        sys.exit(1)

    store = stores[store_type]()
    run(store)

