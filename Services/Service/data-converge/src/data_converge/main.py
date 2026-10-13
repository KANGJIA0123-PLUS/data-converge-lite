"""Command-line argument, output and process error boundary."""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from data_converge.application import Application, CommandResult
from data_converge.config.application import DEFAULT_APP_CONFIG_PATH, StartupError, load_definition
from data_converge.logging_config import LOG_FORMAT, create_file_handler

logger = logging.getLogger(__name__)


def arguments(argv):
    parser = argparse.ArgumentParser(description='Data Converge Lite')
    parser.add_argument('--config', type=Path, default=DEFAULT_APP_CONFIG_PATH)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--validate-only', action='store_true')
    modes.add_argument('--init-state', action='store_true')
    modes.add_argument('--status', action='store_true')
    modes.add_argument('--healthcheck', choices=['liveness', 'readiness'])
    modes.add_argument('--run-once', action='store_true')
    modes.add_argument('--run-task')
    modes.add_argument('--resolve-instance')
    parser.add_argument('--start-time', help='First complete data window; only with --init-state')
    parser.add_argument('--scheduled-time')
    parser.add_argument('--now', help='Explicit evaluation time; only with --run-once')
    parser.add_argument('--resolution', choices=['success', 'retry'])
    parser.add_argument('--note')
    args = parser.parse_args(argv)
    if args.start_time and not args.init_state:
        parser.error('--start-time requires --init-state')
    if args.now and not args.run_once:
        parser.error('--now requires --run-once')
    if args.run_task and not args.scheduled_time:
        parser.error('--run-task requires --scheduled-time')
    if args.resolve_instance and (not args.resolution or not args.note):
        parser.error('--resolve-instance requires --resolution and --note')
    return args


def output(value):
    print(json.dumps(value, ensure_ascii=False, default=str))


def run(args) -> CommandResult:
    application = Application(args.config)
    if args.healthcheck:
        return application.check_health(args.healthcheck)
    if args.status:
        return application.status()
    if args.validate_only:
        return application.validate()
    if args.init_state:
        return application.initialize(args.start_time)
    if args.resolve_instance:
        return application.resolve(args.resolve_instance, args.resolution, args.note)
    if args.run_task:
        return application.run_task(args.run_task, args.scheduled_time)
    if args.run_once:
        return application.run_once(args.now)
    return application.serve()


def main(argv=None):
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args = arguments(argv)
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    file_handler = None
    try:
        file_handler = create_file_handler()
        if file_handler is not None:
            root_logger.addHandler(file_handler)
            root_logger.setLevel(logging.INFO)
        result = run(args)
        if result.payload is not None:
            output(result.payload)
        return result.exit_code
    except (Exception, KeyboardInterrupt) as exc:
        # Untrusted SDK/server/SQL exception bodies never reach application logs.
        detail = str(exc) if isinstance(exc, StartupError) else type(exc).__name__
        logger.error('SERVICE_FAILED: %s', detail)
        return 1
    finally:
        if file_handler is not None:
            root_logger.removeHandler(file_handler)
            file_handler.close()
            root_logger.setLevel(previous_level)


if __name__ == '__main__':
    raise SystemExit(main())
