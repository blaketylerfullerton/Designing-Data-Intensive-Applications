import threading
import json
from typing import Any, Dict, Optional

class KeyValueStateMachine:
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.last_applied = 0

    def apply(self, entry):
        with self.lock:
            command = entry.command
            op = command.get('op')

            if op == 'set':
                self.data[command['key']] = command['value']
                result = {'ok': True}
            elif op == 'get':
                value = self.data.get(command['key'])
                result = {'ok': True, 'value': value}
            elif op == 'delete':
                self.data.pop(command['key'], None)
                result = {'ok': True}
            elif op == 'cas':
                current = self.data.get(command['key'])
                if current == command['expected']:
                    self.data[command['key']] = command['value']
                    result = {'ok': True, 'swapped': True}
                else:
                    result = {'ok': True, 'swapped': False, 'current': current}
            else:
                result = {'ok': False, 'error': 'unknown operation'}

            self.last_applied = entry.index
            return result

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def snapshot(self):
        with self.lock:
            return {
                'data': dict(self.data),
                'last_applied': self.last_applied
            }

    def restore(self, snapshot):
        with self.lock:
            self.data = dict(snapshot['data'])
            self.last_applied = snapshot['last_applied']

class LockStateMachine:
    def __init__(self):
        self.locks: Dict[str, Optional[str]] = {}
        self.lock = threading.Lock()
        self.last_applied = 0

    def apply(self, entry):
        with self.lock:
            command = entry.command
            op = command.get('op')
            lock_name = command.get('lock')
            owner = command.get('owner')

            if op == 'acquire':
                if lock_name not in self.locks or self.locks[lock_name] is None:
                    self.locks[lock_name] = owner
                    result = {'ok': True, 'acquired': True}
                elif self.locks[lock_name] == owner:
                    result = {'ok': True, 'acquired': True, 'already_held': True}
                else:
                    result = {'ok': True, 'acquired': False, 'holder': self.locks[lock_name]}
            elif op == 'release':
                if self.locks.get(lock_name) == owner:
                    self.locks[lock_name] = None
                    result = {'ok': True, 'released': True}
                else:
                    result = {'ok': False, 'error': 'not lock holder'}
            elif op == 'status':
                holder = self.locks.get(lock_name)
                result = {'ok': True, 'lock': lock_name, 'holder': holder}
            else:
                result = {'ok': False, 'error': 'unknown operation'}

            self.last_applied = entry.index
            return result

    def snapshot(self):
        with self.lock:
            return {
                'locks': dict(self.locks),
                'last_applied': self.last_applied
            }

    def restore(self, snapshot):
        with self.lock:
            self.locks = dict(snapshot['locks'])
            self.last_applied = snapshot['last_applied']

class ConfigStateMachine:
    def __init__(self):
        self.config: Dict[str, Any] = {}
        self.version = 0
        self.lock = threading.Lock()
        self.last_applied = 0
        self.watchers = {}

    def apply(self, entry):
        with self.lock:
            command = entry.command
            op = command.get('op')

            if op == 'set':
                key = command['key']
                self.config[key] = command['value']
                self.version += 1
                result = {'ok': True, 'version': self.version}
            elif op == 'get':
                key = command['key']
                value = self.config.get(key)
                result = {'ok': True, 'value': value, 'version': self.version}
            elif op == 'delete':
                key = command['key']
                self.config.pop(key, None)
                self.version += 1
                result = {'ok': True, 'version': self.version}
            elif op == 'list':
                prefix = command.get('prefix', '')
                matching = {k: v for k, v in self.config.items() if k.startswith(prefix)}
                result = {'ok': True, 'config': matching, 'version': self.version}
            elif op == 'batch':
                for item in command.get('ops', []):
                    if item['op'] == 'set':
                        self.config[item['key']] = item['value']
                    elif item['op'] == 'delete':
                        self.config.pop(item['key'], None)
                self.version += 1
                result = {'ok': True, 'version': self.version}
            else:
                result = {'ok': False, 'error': 'unknown operation'}

            self.last_applied = entry.index
            return result

    def snapshot(self):
        with self.lock:
            return {
                'config': dict(self.config),
                'version': self.version,
                'last_applied': self.last_applied
            }

    def restore(self, snapshot):
        with self.lock:
            self.config = dict(snapshot['config'])
            self.version = snapshot['version']
            self.last_applied = snapshot['last_applied']

class LeaderElectionStateMachine:
    def __init__(self):
        self.leaders: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.last_applied = 0

    def apply(self, entry):
        with self.lock:
            command = entry.command
            op = command.get('op')
            group = command.get('group')
            node = command.get('node')

            if op == 'campaign':
                if group not in self.leaders:
                    self.leaders[group] = {'leader': node, 'term': 1}
                    result = {'ok': True, 'elected': True, 'term': 1}
                else:
                    current = self.leaders[group]
                    if current['leader'] is None:
                        self.leaders[group] = {'leader': node, 'term': current['term'] + 1}
                        result = {'ok': True, 'elected': True, 'term': self.leaders[group]['term']}
                    else:
                        result = {'ok': True, 'elected': False, 'current_leader': current['leader']}
            elif op == 'resign':
                if group in self.leaders and self.leaders[group]['leader'] == node:
                    self.leaders[group]['leader'] = None
                    result = {'ok': True, 'resigned': True}
                else:
                    result = {'ok': False, 'error': 'not the leader'}
            elif op == 'heartbeat':
                if group in self.leaders and self.leaders[group]['leader'] == node:
                    result = {'ok': True, 'renewed': True}
                else:
                    result = {'ok': False, 'error': 'not the leader'}
            elif op == 'get_leader':
                if group in self.leaders:
                    result = {'ok': True, 'leader': self.leaders[group]['leader'], 'term': self.leaders[group]['term']}
                else:
                    result = {'ok': True, 'leader': None}
            else:
                result = {'ok': False, 'error': 'unknown operation'}

            self.last_applied = entry.index
            return result

    def snapshot(self):
        with self.lock:
            return {
                'leaders': dict(self.leaders),
                'last_applied': self.last_applied
            }

    def restore(self, snapshot):
        with self.lock:
            self.leaders = dict(snapshot['leaders'])
            self.last_applied = snapshot['last_applied']

