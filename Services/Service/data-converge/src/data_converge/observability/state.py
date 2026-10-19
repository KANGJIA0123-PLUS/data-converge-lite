"""Bounded startup evidence, without raw configuration or ledger metadata."""

import re
from pathlib import Path


def _safe_fingerprint(value: str | None) -> str | None:
    if isinstance(value, str) and re.fullmatch(r'[0-9a-fA-F]{64}', value):
        return value
    return None


def binding_details(*, config_path: Path, state_path: Path, task_count: int,
                    current: str, stored: str | None, coverage: str | None) -> dict:
    reasons = []
    if not stored:
        reasons.append('MISSING_BINDING')
    elif stored != current:
        reasons.append('BINDING_MISMATCH')
    if not coverage:
        reasons.append('MISSING_COVERAGE_START')
    return {
        'config_path': str(config_path)[:4096], 'state_path': str(state_path)[:4096],
        'current_task_count': task_count, 'current_binding': _safe_fingerprint(current),
        'stored_binding': _safe_fingerprint(stored), 'stored_binding_present': bool(stored),
        'coverage_start_present': bool(coverage), 'reasons': reasons,
    }
