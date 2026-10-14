"""Explicit, isolated connection probes using the existing authentication paths."""

from contextlib import closing

from data_converge.dwd_ready import redis_source
from data_converge.errors import safe_error_details
from data_converge.sql_execution import client as sql_client
from data_converge.sql_execution import config as sql_config


def _result(check_id, status, summary, evidence=None, action=''):
    return {'id': check_id, 'status': status, 'summary': summary,
            'evidence': evidence or {}, 'action': action}


def _failed(check_id, summary, error, evidence, action):
    return _result(check_id, 'FAIL', summary,
                   {**evidence, **safe_error_details(error)}, action)


def _clickhouse_checks(config):
    connection = config['clickhouse']
    evidence = {'endpoint': sql_config.connection_origin(connection),
                'database': connection['database'], 'credential_user': connection['username']}
    try:
        resolved = sql_config.resolve_credentials(config)
    except Exception as error:
        return [_failed('clickhouse.credentials', 'ClickHouse 凭据加载失败', error, evidence,
                        '检查密码来源、挂载文件权限及 XMS 解密环境。'),
                _result('clickhouse.query', 'SKIP', '凭据不可用，未发送查询')]
    results = [_result('clickhouse.credentials', 'PASS', 'ClickHouse 凭据加载完成', evidence)]
    try:
        rows = sql_client.execute_sql(resolved, 'SELECT 1', timeout=10)
        if rows != [[1]]:
            raise ValueError('CLICKHOUSE_DIAGNOSTIC_RESPONSE_INVALID')
        results.append(_result('clickhouse.query', 'PASS', 'ClickHouse 只读查询成功',
                               {**evidence, 'timeout_seconds': 10}))
    except Exception as error:
        results.append(_failed('clickhouse.query', 'ClickHouse 只读查询失败', error, evidence,
                               '检查端点、网络、TLS 信任及账号访问权限；凭 query_id 核对服务端日志。'))
    return results


def _redis_evidence(ready):
    connection = ready['redis']
    keys = ('mode', 'db', 'host', 'port', 'service_name', 'ssl', 'ssl_verify')
    return {**{key: connection[key] for key in keys if key in connection},
            'credential_user': connection.get('username')}


def _redis_inspection(source, evidence):
    try:
        with closing(source):
            metadata = source.inspect()
        evidence = {**evidence, **metadata}
        wrong_types = any(metadata[name]['type'] not in ('list', 'none')
                          for name in ('queue', 'processing'))
        if not metadata['ping'] or wrong_types:
            return _result('redis.inspect', 'FAIL', 'Redis 连通状态或队列类型异常', evidence,
                           '核对 Redis 服务及两个键的类型；不要删除或覆盖已有队列。')
        return _result('redis.inspect', 'PASS', 'Redis PING 与两个队列检查通过', evidence)
    except Exception as error:
        return _failed('redis.inspect', 'Redis 只读检查失败', error, evidence,
                       '检查网络、TLS、认证及 PING/TYPE/LLEN 权限；不要消费消息进行测试。')


def _redis_checks(config):
    ready = config['dwd_ready']
    if not ready['enabled']:
        return [_result('redis.create', 'SKIP', 'DWD Redis 信号未启用'),
                _result('redis.inspect', 'SKIP', 'DWD Redis 信号未启用')]
    evidence = _redis_evidence(ready)
    try:
        source = redis_source.create_source(ready)
    except Exception as error:
        return [_failed('redis.create', 'Redis 凭据或客户端创建失败', error, evidence,
                        '根据 error_code 检查凭据、PMS 账号选择、PSM 密钥与 SDK 或 Redis 配置。'),
                _result('redis.inspect', 'SKIP', 'Redis 客户端不可用，未检查队列')]
    return [_result('redis.create', 'PASS', 'Redis 凭据加载与客户端创建完成', evidence),
            _redis_inspection(source, evidence)]


def check_connections(definition) -> list[dict]:
    """Run only on explicit request; never bind state, execute tasks or consume Redis."""
    results = []
    for name, check in (('clickhouse', _clickhouse_checks), ('redis', _redis_checks)):
        try:
            results.extend(check(definition.config))
        except Exception as error:
            results.append(_failed(name + '.connection', '连接诊断未完成', error, {},
                                   '根据安全错误类型检查该组件配置和诊断环境。'))
    return results
