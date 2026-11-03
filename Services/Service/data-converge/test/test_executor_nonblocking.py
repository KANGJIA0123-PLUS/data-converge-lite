"""Long tasks must not stall dispatch; per-task parallelism caps batch overlap."""

from contextlib import closing
from datetime import timedelta
import threading
import time

from data_converge.dag import DAGExecutor, TaskStatus

from .test_state import NOW, graph, open_store


def test_running_task_does_not_block_a_later_round(tmp_path):
    builder, dag = graph()
    release, calls = threading.Event(), []

    def runner(instance):
        calls.append(instance.instance_id)
        if len(calls) == 1:
            release.wait(5)

    with closing(open_store(tmp_path / 'state.db')) as store:
        executor = DAGExecutor(dag, runner, state_store=store, max_concurrent_tasks=2,
                               task_parallelism=2)
        first = builder.create_instance(dag, 'root', NOW)
        executor.execute([first], now=NOW, wait=False)
        second = builder.create_instance(dag, 'root', NOW + timedelta(minutes=5))
        started = time.monotonic()
        executor.execute([second], now=NOW + timedelta(minutes=5), wait=False)
        deadline = time.monotonic() + 2
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(calls) == 2 and time.monotonic() - started < 2
        release.set()
        executor.drain()
        assert store.get_status(first.instance_id) == TaskStatus.SUCCESS
        assert store.get_status(second.instance_id) == TaskStatus.SUCCESS


def test_task_parallelism_limits_concurrent_batches_of_one_task(tmp_path):
    builder, dag = graph()
    active, peak, lock = [0], [0], threading.Lock()

    def runner(_instance):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.05)
        with lock:
            active[0] -= 1

    with closing(open_store(tmp_path / 'state.db')) as store:
        instances = [builder.create_instance(dag, 'root', NOW + timedelta(minutes=5 * n))
                     for n in range(6)]
        executor = DAGExecutor(dag, runner, state_store=store, max_concurrent_tasks=4,
                               task_parallelism=2)
        report = executor.execute(instances, now=NOW + timedelta(minutes=30))
        assert len(report.succeeded) == 6 and peak[0] == 2


def test_default_parallelism_runs_one_batch_per_task(tmp_path):
    builder, dag = graph()
    active, peak, lock = [0], [0], threading.Lock()

    def runner(_instance):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.02)
        with lock:
            active[0] -= 1

    with closing(open_store(tmp_path / 'state.db')) as store:
        instances = [builder.create_instance(dag, 'root', NOW + timedelta(minutes=5 * n))
                     for n in range(3)]
        executor = DAGExecutor(dag, runner, state_store=store, max_concurrent_tasks=4)
        assert len(executor.execute(instances, now=NOW + timedelta(minutes=15)).succeeded) == 3
        assert peak[0] == 1
