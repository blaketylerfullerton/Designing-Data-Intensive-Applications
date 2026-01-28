import threading
import time
from transaction import TransactionManager, IsolationLevel

def dirty_read_anomaly():
    print('=== dirty read ===')
    tm = TransactionManager(IsolationLevel.READ_UNCOMMITTED)

    t1 = tm.begin()
    t1.write('x', 100)
    t1.commit()

    t2 = tm.begin()
    t2.write('x', 200)

    t3 = tm.begin()
    value = t3.read('x')
    print(f't3 reads uncommitted value: {value}')

    t2.abort()

    value_after = t3.read('x')
    print(f't3 reads after abort: {value_after}')
    t3.commit()

def non_repeatable_read_anomaly():
    print('\n=== non-repeatable read ===')
    tm = TransactionManager(IsolationLevel.READ_COMMITTED)

    setup = tm.begin()
    setup.write('x', 100)
    setup.commit()

    results = []
    barrier = threading.Barrier(2)

    def reader():
        t = tm.begin()
        v1 = t.read('x')
        results.append(('read1', v1))
        barrier.wait()
        time.sleep(0.1)
        v2 = t.read('x')
        results.append(('read2', v2))
        t.commit()

    def writer():
        barrier.wait()
        t = tm.begin()
        t.write('x', 200)
        t.commit()

    threads = [threading.Thread(target=reader), threading.Thread(target=writer)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f'results: {results}')
    print(f'non-repeatable: {results[0][1]} != {results[1][1]}')

def phantom_read_anomaly():
    print('\n=== phantom read ===')
    tm = TransactionManager(IsolationLevel.READ_COMMITTED)

    setup = tm.begin()
    setup.write('user:1', {'name': 'alice', 'age': 25})
    setup.write('user:2', {'name': 'bob', 'age': 30})
    setup.commit()

    results = []
    barrier = threading.Barrier(2)

    def scanner():
        t = tm.begin()
        count1 = 0
        for i in range(1, 10):
            val = t.read(f'user:{i}')
            if val is not None:
                count1 += 1
        results.append(('count1', count1))
        barrier.wait()
        time.sleep(0.1)
        count2 = 0
        for i in range(1, 10):
            val = t.read(f'user:{i}')
            if val is not None:
                count2 += 1
        results.append(('count2', count2))
        t.commit()

    def inserter():
        barrier.wait()
        t = tm.begin()
        t.write('user:3', {'name': 'charlie', 'age': 35})
        t.commit()

    threads = [threading.Thread(target=scanner), threading.Thread(target=inserter)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f'results: {results}')

def write_skew_anomaly():
    print('\n=== write skew ===')
    tm = TransactionManager(IsolationLevel.SNAPSHOT)

    setup = tm.begin()
    setup.write('alice_balance', 100)
    setup.write('bob_balance', 100)
    setup.commit()

    barrier = threading.Barrier(2)
    results = []

    def withdraw_alice():
        t = tm.begin()
        alice = t.read('alice_balance')
        bob = t.read('bob_balance')
        total = alice + bob
        results.append(('alice_check', total))
        barrier.wait()
        if total >= 150:
            t.write('alice_balance', alice - 150)
        t.commit()

    def withdraw_bob():
        t = tm.begin()
        alice = t.read('alice_balance')
        bob = t.read('bob_balance')
        total = alice + bob
        results.append(('bob_check', total))
        barrier.wait()
        if total >= 150:
            t.write('bob_balance', bob - 150)
        t.commit()

    threads = [threading.Thread(target=withdraw_alice), threading.Thread(target=withdraw_bob)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = tm.begin()
    alice_final = final.read('alice_balance')
    bob_final = final.read('bob_balance')
    final.commit()

    print(f'checks: {results}')
    print(f'final: alice={alice_final}, bob={bob_final}, total={alice_final + bob_final}')
    print(f'invariant violated: {alice_final + bob_final < 0}')

def lost_update_anomaly():
    print('\n=== lost update ===')
    tm = TransactionManager(IsolationLevel.READ_COMMITTED)

    setup = tm.begin()
    setup.write('counter', 0)
    setup.commit()

    def increment():
        t = tm.begin()
        current = t.read('counter')
        time.sleep(0.01)
        try:
            t.write('counter', current + 1)
            t.commit()
            return True
        except:
            t.abort()
            return False

    threads = []
    for _ in range(10):
        threads.append(threading.Thread(target=increment))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = tm.begin()
    value = final.read('counter')
    final.commit()

    print(f'expected: 10, actual: {value}')
    print(f'lost updates: {10 - value}')

def snapshot_isolation_prevents_lost_update():
    print('\n=== snapshot isolation prevents lost update ===')
    tm = TransactionManager(IsolationLevel.SNAPSHOT)

    setup = tm.begin()
    setup.write('counter', 0)
    setup.commit()

    success_count = [0]
    lock = threading.Lock()

    def increment():
        t = tm.begin()
        current = t.read('counter')
        time.sleep(0.01)
        try:
            t.write('counter', current + 1)
            t.commit()
            with lock:
                success_count[0] += 1
            return True
        except:
            t.abort()
            return False

    threads = []
    for _ in range(10):
        threads.append(threading.Thread(target=increment))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = tm.begin()
    value = final.read('counter')
    final.commit()

    print(f'successful commits: {success_count[0]}')
    print(f'final value: {value}')

if __name__ == '__main__':
    dirty_read_anomaly()
    non_repeatable_read_anomaly()
    phantom_read_anomaly()
    write_skew_anomaly()
    lost_update_anomaly()
    snapshot_isolation_prevents_lost_update()

