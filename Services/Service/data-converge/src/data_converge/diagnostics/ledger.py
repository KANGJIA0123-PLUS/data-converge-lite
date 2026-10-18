"""Bounded queries in a consistent, read-only SQLite snapshot."""
from data_converge.state import StateStore
from data_converge.state import events

PROBLEM_STATUSES = ('UNKNOWN', 'FAILED', 'BLOCKED')


class DiagnosticLedger(StateStore):
    def __init__(self, path):
        super().__init__(path)
        try:
            self._connection().execute('BEGIN')
        except BaseException:
            self.close()
            raise

    def events_available(self):
        return events.available(self._connection())

    def list_execution_events(self, *, after_event=0, limit=20):
        return events.list_events(self._connection(), after_event=after_event, limit=limit)

    def problems(self, *, task=None, limit=20, offset=0):
        clause = "status IN ('UNKNOWN', 'FAILED', 'BLOCKED')"
        parameters = []
        if task is not None:
            clause += ' AND taskname=?'
            parameters.append(task)
        conn = self._connection()
        counts = dict(conn.execute(f'SELECT status, COUNT(*) FROM task_instances WHERE {clause} '
                                   'GROUP BY status', parameters).fetchall())
        rows = conn.execute(f'SELECT * FROM task_instances WHERE {clause} ORDER BY '
                            "CASE status WHEN 'UNKNOWN' THEN 0 WHEN 'FAILED' THEN 1 ELSE 2 END, "
                            'data_end DESC, taskname LIMIT ? OFFSET ?',
                            [*parameters, limit, offset]).fetchall()
        return counts, [dict(row) for row in rows]

    def signals(self, start):
        from datetime import timezone

        if not self._has_dwd_table():
            return []
        stamp = start.astimezone(timezone.utc).isoformat(timespec='microseconds')
        rows = self._connection().execute(
            'SELECT dwd_type, received_at FROM dwd_ready_signals WHERE window_start=? ORDER BY dwd_type',
            (stamp,)).fetchall()
        return [dict(row) for row in rows]

    def progress(self):
        row = self._connection().execute('SELECT MAX(data_end) AS newest_window_end, '
            'MAX(finished_at) AS latest_finished_at, MIN(CASE WHEN status IN '
            "('PENDING','RUNNING','BLOCKED') THEN data_start END) AS oldest_active_window "
            'FROM task_instances').fetchone()
        return dict(row)
