"""Sentinel connection creation integrates with the existing List transport."""

from copy import deepcopy

import pytest

from data_converge.dwd_ready.config import load_ready_config


def sentinel_config(**updates):
    connection = dict(mode='sentinel', db=5, service_name='redis-cluster', username='ossdbuser',
                      password_file='redis-password', root_key_file='redis_root.key',
                      work_key_file='redis_shared.key', ssl=False)
    connection.update(updates)
    return {'dwd_ready':{'enabled':True,'redis':connection}}


def test_offline_sentinel_validation_resolves_paths_without_reading_secrets(tmp_path):
    raw=sentinel_config()
    original=deepcopy(raw)
    config=load_ready_config(raw,tmp_path)['redis']
    assert config['mode']=='sentinel' and config['db']==5
    assert config['password_file']==str(tmp_path/'redis-password')
    assert config['root_key_file']==str(tmp_path/'redis_root.key')
    assert config['work_key_file']==str(tmp_path/'redis_shared.key')
    assert config['service_name']=='redis-cluster' and config['paas_name'] is None
    assert raw==original
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('updates', [{'host':'private'},{'port':26379},{'password_env':'PRIVATE'},
    {'service_name':''},{'service_name':'redis\nprivate'},{'username':None},{'password_file':''},
    {'root_key_file':None},{'work_key_file':''},{'kms_timeout_seconds':0},
    {'kms_timeout_seconds':float('inf')},{'paas_name':''}])
def test_sentinel_rejects_mixed_providers_and_invalid_metadata(tmp_path,updates):
    with pytest.raises(ValueError) as caught:
        load_ready_config(sentinel_config(**updates),tmp_path)
    assert 'private' not in str(caught.value)


def test_direct_modes_reject_ignored_sentinel_fields(tmp_path):
    raw={'dwd_ready':{'enabled':True,'redis':dict(mode='standalone',host='localhost',port=6379,
         password_env='SYNTHETIC_PASSWORD',password_file='private')}}
    with pytest.raises(ValueError):
        load_ready_config(raw,tmp_path)


def test_real_sentinel_pool_uses_distinct_auth_callbacks_and_owned_connections(tmp_path,monkeypatch):
    from data_converge.dwd_ready import redis_source as source_module
    from data_converge.infrastructure.redis import client as connection_module
    from data_converge.dwd_ready.redis_credentials import SentinelCredentials
    config=load_ready_config(sentinel_config(ssl=True),tmp_path)
    monkeypatch.setattr(connection_module,'load_sentinel_credentials',lambda _:SentinelCredentials((('localhost',26379),),'synthetic'))
    monkeypatch.setattr(connection_module,'create_psm_codec',lambda *_: object())
    callbacks=[]
    def callback_factory(codec,*,select_db):
        def callback(connection):
            raise AssertionError('offline pool inspection must not open sockets')
        callbacks.append((select_db,callback))
        return callback
    monkeypatch.setattr(connection_module,'make_connect_callback',callback_factory)
    source=source_module.create_source(config)
    try:
        opts=source._client.connection_pool.connection_kwargs
        manager=source._client.connection_pool.sentinel_manager
        sentinel_opts=manager.sentinels[0].connection_pool.connection_kwargs
        callback_map=dict(callbacks)
        assert opts['redis_connect_func'] is callback_map[True]
        assert sentinel_opts['redis_connect_func'] is callback_map[False]
        assert opts['db']==5 and sentinel_opts.get('db',0)==0
        assert opts.get('username') is None  # PMS selector is not a Redis ACL username.
        assert opts['retry']._retries==0 and sentinel_opts['retry']._retries==0
        assert opts['ssl_cert_reqs']=='required' and opts['ssl_check_hostname'] is True
        assert sentinel_opts['ssl_cert_reqs']=='required' and sentinel_opts['ssl_check_hostname'] is True
        assert source.identity and len(source.identity)==64
    finally:
        source.close()
    assert source._closed


def test_sentinel_identity_change_warns_and_retains_original_binding(tmp_path, caplog):
    from data_converge.dwd_ready.redis_source import bind_sentinel_identity
    from data_converge.state import StateStore
    store = StateStore(tmp_path/'state.sqlite3', create=True)
    store.acquire_lock()
    try:
        bind_sentinel_identity(store,'a'*64)
        bind_sentinel_identity(store,'a'*64)
        bind_sentinel_identity(store,'b'*64)
        bind_sentinel_identity(store, None)
        assert store.get_meta('dwd_redis_sentinel_binding')=='a'*64
        assert caplog.text.count('DWD_REDIS_BINDING_CHECK_DISABLED') == 1
        assert 'DC-STATE-002' in caplog.text
    finally:
        store.close()
