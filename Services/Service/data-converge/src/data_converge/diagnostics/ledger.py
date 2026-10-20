"""Bounded queries in a consistent, read-only SQLite snapshot."""
from data_converge.state import StateStore
from data_converge.state import events

PROBLEM_STATUSES = ('UNKNOWN', 'FAILED', 'BLOCKED')


class DiagnosticLedger(StateStore):
    def __init__(self, path):
        super().__init__(path)
        try:
            self._connection().execute('BEGIN')
            self.validate_retirement()
        except BaseException:
            self.close()
            raise

    def events_available(self):
        return events.available(self._connection())

    def list_execution_events(self, *, after_event=0, limit=20):
        return events.list_events(self._connection(), after_event=after_event, limit=limit)

    def problems(self, *, task=None, limit=20, offset=0):
        clause = "status IN ('UNKNOWN', 'FAILED', 'BLOCKED') AND " + self._active_clause()
        parameters = []
        if task is not None:
            clause += ' AND taskname=?'
            parameters.append(task)
        conn = self._connection()
        counts = dict(conn.execute(f'SELECT status, COUNT(*) FROM task_instances WHERE {clause} '
                                   'GROUP BY status', parameters).fetchall())
        rows = conn.execute(f'SELECT * FROM task_instances WHERE {clause} ORDER BY '
                            "CASE status WHEN 'UNKNOWN' THEN 0 WHEN 'FAILED' THEN 1 ELSE 2 END, "
                            'julianday(data_end) DESC, taskname LIMIT ? OFFSET ?',
                            [*parameters, limit, offset]).fetchall()
        return counts, [dict(row) for row in rows]

    def _active_clause(self):
        if self.get_meta('retirement_required') is None:
            return '1=1'
        return ('NOT EXISTS (SELECT 1 FROM retired_instances WHERE '
                'retired_instances.instance_id=task_instances.instance_id)')

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
        active = self._active_clause()
        row = self._connection().execute(f"""
            SELECT
              (SELECT data_end FROM task_instances WHERE {active}
               ORDER BY julianday(data_end) DESC LIMIT 1) AS newest_window_end,
              (SELECT finished_at FROM task_instances WHERE {active}
               ORDER BY julianday(finished_at) DESC LIMIT 1) AS latest_finished_at,
              (SELECT data_start FROM task_instances WHERE {active}
               AND status IN ('PENDING','RUNNING','BLOCKED')
               ORDER BY julianday(data_start) ASC LIMIT 1) AS oldest_active_window
        """).fetchone()
        return dict(row)
