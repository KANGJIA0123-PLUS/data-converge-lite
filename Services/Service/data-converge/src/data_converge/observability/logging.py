"""Own the application's logging resources without changing other handlers."""

from contextlib import AbstractContextManager
import logging
import os
from pathlib import Path

from .handlers import RetentionFileHandler
from .settings import LoggingSettings


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
    return None


def create_file_handler(settings: LoggingSettings | None = None) -> RetentionFileHandler | None:
    path = log_file_path()
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RetentionFileHandler(path, settings)
    handler.setLevel(logging.DEBUG if settings and settings.debug_enabled else logging.INFO)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


class LoggingSession(AbstractContextManager):
    """Own file output and service verbosity for one command's lifetime."""

    def __init__(self):
        self._root = logging.getLogger()
        self._previous_level = self._root.level
        self._application = logging.getLogger('data_converge')
        self._previous_application_level = self._application.level
        self._handler = None

    def __enter__(self):
        # Until configuration is readable, append diagnostics without aging files.
        self._replace_handler(create_file_handler())
        return self

    def configure(self, settings: LoggingSettings) -> None:
        self._replace_handler(create_file_handler(settings))
        self._application.setLevel(logging.DEBUG if settings.debug_enabled else logging.INFO)

    def _replace_handler(self, handler) -> None:
        previous = self._handler
        if handler is not None:
            self._root.addHandler(handler)
            self._root.setLevel(logging.INFO)
        self._handler = handler
        if previous is not None:
            self._close_handler(previous)

    def _close_handler(self, handler) -> None:
        self._root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            # Cleanup must not replace the command's result or expose I/O details.
            logging.getLogger(__name__).error('LOGGING_CLOSE_FAILED')

    def __exit__(self, *_):
        try:
            if self._handler is not None:
                handler = self._handler
                self._handler = None
                self._close_handler(handler)
        finally:
            self._root.setLevel(self._previous_level)
            self._application.setLevel(self._previous_application_level)
        return False
