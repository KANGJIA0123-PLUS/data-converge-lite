"""接管会在替换共享状态前取消所有 RUNNING query。"""

from contextlib import closing

from data_converge.dag import DAGExecutor, TaskStatus
from data_converge.state.shared import _take_over

from .test_mirror_recovery import FakeMirror, attach, pending_instance
from .test_state import NOW, graph, open_store


class RunningFakeMirror(FakeMirror):
    def running_instances(self):
        return [row | {'instance_id': instance_id}
                for instance_id, row in self.rows.items()
                if row.get('status') == 'RUNNING']


def test_takeover_kills_running_query_when_local_is_pending(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = RunningFakeMirror()
    killed = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        store.enqueue_and_advance([instance], {})
        mirror.rows[instance.instance_id] |= {'status': 'RUNNING', 'query_id': 'old-query'}
        mirror.cursors = {'root': NOW}
        _take_over(store, mirror, killed.extend)
    assert killed == ['old-query']
    assert mirror.rows[instance.instance_id]['status'] == TaskStatus.PENDING.value


def test_takeover_kills_running_query_before_local_success_wins(tmp_path):
    builder, dag = graph()
    instance = pending_instance(builder, dag)
    mirror = RunningFakeMirror()
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        DAGExecutor(dag, lambda _item: None, state_store=store).execute([instance], now=NOW)
    mirror.rows[instance.instance_id] |= {'status': 'RUNNING', 'query_id': 'old-query'}
    mirror.cursors = {'root': NOW}
    killed = []
    with closing(open_store(tmp_path / 'state.db')) as store:
        attach(store, mirror)
        _take_over(store, mirror, killed.extend)
    assert killed == ['old-query']
    assert mirror.rows[instance.instance_id]['status'] == TaskStatus.SUCCESS.value
