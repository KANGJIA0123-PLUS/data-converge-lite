"""Durable submission inbox independent of the service's single-writer ledger."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import uuid


TERMINAL_STATUSES = frozenset({'SUCCESS', 'SKIPPED', 'FAILED', 'UNKNOWN', 'BLOCKED'})
TOKEN_PATTERN = r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}'


class BackfillQueueError(ValueError):
    """Fixed public request or progress error."""


def validate_request_id(value):
    if value is not None and (not isinstance(value, str) or not re.fullmatch(TOKEN_PATTERN, value)):
        raise BackfillQueueError('BACKFILL_REQUEST_ID_INVALID')


def _serialize_plan(plan):
    items = plan.get('items') if isinstance(plan, dict) else None
    if not isinstance(items, list) or not items:
        raise BackfillQueueError('BACKFILL_PLAN_INVALID')
    identities = [item.get('instance_id') if isinstance(item, dict) else None for item in items]
    if (not all(isinstance(value, str) and value for value in identities)
            or len(set(identities)) != len(identities)
            or not isinstance(plan.get('revision'), str)
            or not plan['revision'] or not isinstance(plan.get('recompute'), bool)
            or plan.get('total') != len(items)):
        raise BackfillQueueError('BACKFILL_PLAN_INVALID')
    payload = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return payload, hashlib.sha256(payload.encode('utf-8')).hexdigest()


def _job_status(counts):
    total = sum(counts.values())
    remaining = counts.get('QUEUED', 0)
    if remaining:
        return 'QUEUED' if remaining == total else 'IN_PROGRESS'
    return 'COMPLETED_WITH_ERRORS' if any(counts.get(s) for s in ('FAILED', 'UNKNOWN', 'BLOCKED')) else 'COMPLETED'


def _loaded_plan(raw):
    plan = json.loads(raw)
    if (not isinstance(plan, dict) or type(plan.get('schema_version')) is not int
            or plan['schema_version'] != 1 or not isinstance(plan.get('revision'), str)
            or not plan['revision'] or not isinstance(plan.get('recompute'), bool)
            or type(plan.get('total')) is not int or plan['total'] < 1
            or not isinstance(plan.get('items'), list) or len(plan['items']) != plan['total']):
        raise BackfillQueueError('BACKFILL_QUEUE_INVALID')
    return plan


def _loaded_item(raw):
    item = json.loads(raw)
    strings = ('instance_id', 'taskname', 'scheduled_time', 'data_start', 'data_end')
    if (not isinstance(item, dict) or type(item.get('ordinal')) is not int or item['ordinal'] < 0
            or not all(isinstance(item.get(key), str) and item[key] for key in strings)
            or not isinstance(item.get('upstream_instance_ids'), list)
            or not all(isinstance(value, str) and value for value in item['upstream_instance_ids'])):
        raise BackfillQueueError('BACKFILL_QUEUE_INVALID')
    return item


def _pending_row(row):
    try:
        plan, item = _loaded_plan(row['plan']), _loaded_item(row['payload'])
        ordinal = row['ordinal']
        if (type(ordinal) is not int or not 0 <= ordinal < plan['total']
                or item != plan['items'][ordinal] or item['ordinal'] != ordinal
                or row['instance_id'] != item['instance_id']
                or not isinstance(row['job_id'], str) or not re.fullmatch(TOKEN_PATTERN, row['job_id'])
                or row['item_id'] != f"{row['job_id']}:{ordinal}"):
            raise BackfillQueueError('BACKFILL_QUEUE_INVALID')
        return dict(item, job_id=row['job_id'], item_id=row['item_id'],
                    ordinal=ordinal, revision=plan['revision'], recompute=plan['recompute'])
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
        raise BackfillQueueError('BACKFILL_QUEUE_INVALID') from None


class Queue:
    """Short transactions allow CLI submission while the service owns its ledger.

    Existing queues open without initialization or file creation. Items remain
    QUEUED until the worker records a terminal result; execution admission and
    crash deduplication belong to the main ledger, not to this inbox.
    """

    def __init__(self, path, *, create=False, read_only=False):
        self.path = Path(path)
        if create and read_only:
            raise BackfillQueueError('BACKFILL_QUEUE_MODE_INVALID')
        if self.path.is_symlink():
            raise BackfillQueueError('BACKFILL_QUEUE_INVALID')
        if not create and not self.path.is_file():
            raise FileNotFoundError(self.path)
        mode = 'ro' if read_only else 'rwc' if create else 'rw'
        uri = self.path.resolve().as_uri() + '?mode=' + mode
        self.connection = sqlite3.connect(uri, uri=True, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        try:
            self.connection.execute('PRAGMA foreign_keys=ON')
            if create:
                self.connection.execute('PRAGMA journal_mode=WAL')
                self._initialize()
        except BaseException:
            self.connection.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self.connection.close()

    @contextmanager
    def _transaction(self, *, write=True):
        self.connection.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        try:
            yield self.connection
            self.connection.execute('COMMIT')
        except BaseException:
            self.connection.execute('ROLLBACK')
            raise

    def _initialize(self):
        with self._transaction() as connection:
            connection.execute('''CREATE TABLE IF NOT EXISTS backfill_jobs (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL UNIQUE,
                request_id TEXT UNIQUE, created_at TEXT NOT NULL, plan TEXT NOT NULL,
                plan_digest TEXT NOT NULL)''')
            connection.execute('''CREATE TABLE IF NOT EXISTS backfill_items (
                item_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES backfill_jobs(job_id),
                ordinal INTEGER NOT NULL, instance_id TEXT NOT NULL, payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'QUEUED', query_id TEXT, error TEXT, finished_at TEXT,
                UNIQUE(job_id, ordinal), UNIQUE(job_id, instance_id))''')
            connection.execute('CREATE INDEX IF NOT EXISTS backfill_pending '
                               'ON backfill_items(status, job_id, ordinal)')

    def submit(self, plan, request_id=None):
        validate_request_id(request_id)
        payload, digest = _serialize_plan(plan)
        with self._transaction() as connection:
            existing = connection.execute(
                'SELECT job_id, plan_digest FROM backfill_jobs WHERE request_id=?', (request_id,)).fetchone()
            if existing:
                if existing['plan_digest'] != digest:
                    raise BackfillQueueError('BACKFILL_REQUEST_ID_CONFLICT')
                return existing['job_id']
            job_id = 'bf_' + uuid.uuid4().hex
            connection.execute(
                'INSERT INTO backfill_jobs(job_id, request_id, created_at, plan, plan_digest) VALUES(?,?,?,?,?)',
                (job_id, request_id, datetime.now(timezone.utc).isoformat(), payload, digest))
            connection.executemany(
                'INSERT INTO backfill_items(item_id, job_id, ordinal, instance_id, payload) VALUES(?,?,?,?,?)',
                [(f'{job_id}:{number}', job_id, number, item['instance_id'],
                  json.dumps(item, ensure_ascii=False, sort_keys=True))
                 for number, item in enumerate(plan['items'])])
        return job_id

    def next_item(self):
        row = self.connection.execute('''SELECT i.*, j.plan FROM backfill_items i
            JOIN backfill_jobs j ON j.job_id=i.job_id WHERE i.status='QUEUED'
            ORDER BY j.sequence, i.ordinal LIMIT 1''').fetchone()
        if row is None:
            return None
        return _pending_row(row)

    def complete(self, item_id, status, query_id=None, error=None):
        if status not in TERMINAL_STATUSES:
            raise BackfillQueueError('BACKFILL_STATUS_INVALID')
        with self._transaction() as connection:
            existing = connection.execute(
                'SELECT status FROM backfill_items WHERE item_id=?', (item_id,)).fetchone()
            if existing is None:
                raise BackfillQueueError('BACKFILL_ITEM_NOT_FOUND')
            if existing['status'] != 'QUEUED':
                if existing['status'] != status:
                    raise BackfillQueueError('BACKFILL_ITEM_ALREADY_COMPLETED')
                return
            connection.execute('''UPDATE backfill_items SET status=?, query_id=?, error=?, finished_at=?
                WHERE item_id=?''', (status, query_id, error, datetime.now(timezone.utc).isoformat(), item_id))

    def item_status(self, job_id, instance_id):
        row = self.connection.execute(
            'SELECT status FROM backfill_items WHERE job_id=? AND instance_id=?',
            (job_id, instance_id)).fetchone()
        return row['status'] if row else None

    def status(self, job_id, limit=20, offset=0):
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise BackfillQueueError('BACKFILL_PAGINATION_INVALID')
        with self._transaction(write=False):
            return self._status_snapshot(job_id, limit, offset)

    def _status_snapshot(self, job_id, limit, offset):
        row = self.connection.execute(
            'SELECT * FROM backfill_jobs WHERE job_id=?', (job_id,)).fetchone()
        if row is None:
            raise BackfillQueueError('BACKFILL_JOB_NOT_FOUND')
        plan = json.loads(row['plan'])
        counts = {item['status']: item['count'] for item in self.connection.execute(
            'SELECT status, COUNT(*) AS count FROM backfill_items WHERE job_id=? GROUP BY status', (job_id,))}
        total = sum(counts.values())
        items = self.connection.execute('''SELECT * FROM backfill_items WHERE job_id=?
            ORDER BY ordinal LIMIT ? OFFSET ?''', (job_id, limit, offset)).fetchall()
        return dict(job_id=job_id, request_id=row['request_id'], created_at=row['created_at'],
                    status=_job_status(counts), total=total, completed=total-counts.get('QUEUED', 0),
                    counts=counts, metadata={key: value for key, value in plan.items() if key != 'items'},
                    items=[self._public_item(item) for item in items], limit=limit, offset=offset,
                    has_more=offset + len(items) < total)

    @staticmethod
    def _public_item(row):
        return dict(json.loads(row['payload']), item_id=row['item_id'], ordinal=row['ordinal'],
                    status=row['status'], query_id=row['query_id'], error=row['error'],
                    finished_at=row['finished_at'])

    def jobs(self, limit=20):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise BackfillQueueError('BACKFILL_PAGINATION_INVALID')
        ids = self.connection.execute(
            'SELECT job_id FROM backfill_jobs ORDER BY sequence DESC LIMIT ?', (limit,)).fetchall()
        return [self.status(row['job_id'], limit=1) for row in ids]
