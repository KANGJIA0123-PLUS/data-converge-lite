"""Route service logs to the platform directory and its external rotation."""

import logging
from logging.handlers import WatchedFileHandler
import os
from pathlib import Path


LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


def log_file_path() -> Path | None:
    service_name = os.environ.get('SERVICE_NAME') or 'data-converge'
    if directory := os.environ.get('LOG_PATH'):
        return Path(directory) / f'{service_name}.log'
    # Deployment supplies a full filename; service.conf supplies a directory.
    if filename := os.environ.get('SERVICE_LOG_PATH'):
        return Path(filename)
    if product_name := os.environ.get('PRODUCT_NAME'):
        return Path('/srv/its') / product_name / service_name / 'log' / f'{service_name}.log'
    # Local CLI commands without platform settings continue to use stderr.
    return None


def create_file_handler() -> WatchedFileHandler | None:
    path = log_file_path()
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = WatchedFileHandler(path, encoding='utf-8')
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler
