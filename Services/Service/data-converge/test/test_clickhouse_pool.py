"""Aggregation pool: settings validation, query parameters and bounded concurrency."""

import threading
import time

import pytest

from data_converge.sql_execution.pool import ClickHousePool, ClickHousePoolError


def test_design_defaults():
    pool = ClickHousePool.from_mapping(None)
    assert pool.max_connections == 20 and pool.connect_timeout_seconds == 10
    assert pool.query_parameters() == {'max_threads': '8', 'readonly': '0', 'load_balancing': 'random',
                                       'max_memory_usage': '10000000000'}


def test_partial_settings_keep_other_defaults():
    pool = ClickHousePool.from_mapping({'settings': {'max_threads': 4}})
    assert pool.query_parameters()['max_threads'] == '4'
    assert pool.query_parameters()['readonly'] == '0'


@pytest.mark.parametrize('value', [
    {'max_connections': 0}, {'connect_timeout_seconds': 0}, {'settings': {'max_execution_time': 1}},
    {'settings': {'load_balancing': "random' OR 1"}}, {'settings': {'max_threads': -1}}, {'other': 1},
])
def test_invalid_settings_rejected(value):
    with pytest.raises(ClickHousePoolError):
        ClickHousePool.from_mapping(value)


def test_slot_limits_concurrency_and_times_out():
    pool = ClickHousePool(max_connections=2)
    active, peak, lock = [0], [0], threading.Lock()

    def work():
        with pool.slot(5):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.05)
            with lock:
                active[0] -= 1

    threads = [threading.Thread(target=work) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert peak[0] == 2
    with pool.slot(1), pool.slot(1):
        with pytest.raises(TimeoutError):
            with pool.slot(0.05):
                pass


def test_pool_settings_are_sent_and_exhaustion_is_retryable(monkeypatch):
    from data_converge.sql_execution import client

    seen = {}

    def fake(config, sql, timeout, start, end, query_id, settings, connect_timeout):
        seen.update(settings=settings, connect_timeout=connect_timeout, timeout=timeout)
        return []

    monkeypatch.setattr(client, '_execute', fake)
    pool = ClickHousePool(max_connections=1)
    client.execute_sql({}, 'SELECT 1', timeout=600, pool=pool)
    assert seen['settings']['max_threads'] == '8' and seen['connect_timeout'] == 10
    with pool.slot(1):
        with pytest.raises(client.SQLExecutionError) as caught:
            client.execute_sql({}, 'SELECT 1', timeout=0.05, pool=pool)
    assert caught.value.retryable and not caught.value.outcome_unknown
