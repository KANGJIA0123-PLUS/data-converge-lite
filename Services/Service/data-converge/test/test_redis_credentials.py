"""Synthetic PMS documents exercise Redis service/account and endpoint selection."""

import base64
import json
import sys

import pytest


def envelope(items):
    return base64.b64encode(json.dumps({'paasconfig': items}).encode()).decode()


def service(**updates):
    item = {'name': 'redis-pms', 'IPS': 'sentinel-a:26379, [::1]:26380',
            'info': [{'user': 'ossdbuser', 'password': ' exact synthetic password '}]}
    item.update(updates)
    return item


def test_selects_unique_redis_service_and_exact_account_without_first_item_assumption():
    from data_converge.infrastructure.redis.credentials import parse_sentinel_credentials
    document = envelope([{'name': 'unrelated', 'info': []}, service()])
    result = parse_sentinel_credentials(document, 'ossdbuser')
    assert result.sentinels == (('sentinel-a', 26379), ('::1', 26380))
    assert result.password == ' exact synthetic password '
    assert 'password' not in repr(result)


def test_service_name_disambiguates_but_duplicate_names_or_accounts_do_not():
    from data_converge.infrastructure.redis.credentials import parse_sentinel_credentials
    two = [service(name='redis-one'), service(name='redis-two', IPS='other:26379')]
    assert parse_sentinel_credentials(envelope(two), 'ossdbuser', 'redis-two').sentinels == (('other', 26379),)
    for items, name in [(two, None), ([service(), service()], 'redis-pms'),
                        ([service(info=[{'user':'ossdbuser','password':'a'}, {'user':'ossdbuser','password':'b'}])], None)]:
        with pytest.raises(ValueError, match='DWD_REDIS_PMS_INVALID'):
            parse_sentinel_credentials(envelope(items), 'ossdbuser', name)


@pytest.mark.parametrize('ips', ['', 'host', 'host:0', 'host:65536', 'user:secret@host:26379',
                                 'redis://host:26379', 'host:26379,', '::1:26379',
                                 '[not-ip]:26379', 'host:26379,host:26379',
                                 ','.join(f'host{i}:26379' for i in range(9))])
def test_invalid_or_excessive_endpoints_fail_without_echoing_input(ips):
    from data_converge.infrastructure.redis.credentials import parse_sentinel_credentials
    with pytest.raises(ValueError) as caught:
        parse_sentinel_credentials(envelope([service(IPS=ips)]), 'ossdbuser')
    assert str(caught.value) == 'DWD_REDIS_PMS_INVALID'
    assert caught.value.__suppress_context__


@pytest.mark.parametrize('value', ['not base64', '', base64.b64encode(b'{"paasconfig":[],"paasconfig":[]}').decode(),
                                 envelope([service(info=[{'user':'other','password':'private'}])]),
                                 envelope([service(info=[{'user':'ossdbuser','password':''}])]),
                                 envelope([service(info=[{'user':'ossdbuser','password':'private\0'}])])])
def test_invalid_pms_is_redacted(value):
    from data_converge.infrastructure.redis.credentials import parse_sentinel_credentials
    with pytest.raises(ValueError) as caught:
        parse_sentinel_credentials(value, 'ossdbuser')
    assert str(caught.value) == 'DWD_REDIS_PMS_INVALID'


def test_password_rotation_does_not_change_logical_queue_identity():
    from data_converge.infrastructure.redis.credentials import parse_sentinel_credentials, sentinel_identity
    connection = {'service_name':'redis-cluster','db':5,'username':'ossdbuser'}
    first = parse_sentinel_credentials(envelope([service()]), 'ossdbuser')
    rotated = parse_sentinel_credentials(envelope([service(IPS='[::1]:26380,sentinel-a:26379',
        info=[{'user':'ossdbuser','password':'rotated'}])]), 'ossdbuser')
    assert sentinel_identity(connection, first) == sentinel_identity(connection, rotated)
    changed = parse_sentinel_credentials(envelope([service(IPS='other:26379')]), 'ossdbuser')
    assert sentinel_identity(connection, changed) != sentinel_identity(connection, first)


def test_load_uses_bounded_existing_xms_helper_once_without_mutating_config(tmp_path, monkeypatch):
    from data_converge.infrastructure.redis import credentials
    path = tmp_path / 'password'
    path.write_bytes(b'synthetic encrypted document\n')
    connection = {'password_file':str(path),'username':'ossdbuser','paas_name':None,'kms_timeout_seconds':4}
    calls=[]
    def decrypt(command, ciphertext, timeout):
        calls.append((command, ciphertext, timeout))
        return envelope([service()])
    monkeypatch.setattr(credentials, 'run_decryptor', decrypt)
    result = credentials.load_sentinel_credentials(connection)
    assert result.sentinels[0] == ('sentinel-a',26379)
    assert calls == [([sys.executable,'-m','data_converge.sql_execution.kms_worker'],
                      b'synthetic encrypted document\n',4)]
    assert 'password' not in connection


def test_missing_secret_or_sdk_failure_is_redacted(tmp_path, monkeypatch):
    from data_converge.infrastructure.redis import credentials
    config = {'password_file':str(tmp_path/'missing'), 'username':'ossdbuser','kms_timeout_seconds':1}
    with pytest.raises(ValueError, match='DWD_REDIS_CREDENTIAL_FAILED'):
        credentials.load_sentinel_credentials(config)
    (tmp_path/'missing').write_text('synthetic')
    def broken(*_):
        raise RuntimeError('private password from SDK')
    monkeypatch.setattr(credentials, 'run_decryptor', broken)
    with pytest.raises(ValueError) as caught:
        credentials.load_sentinel_credentials(config)
    assert str(caught.value) == 'DWD_REDIS_CREDENTIAL_FAILED'
    assert caught.value.__suppress_context__
