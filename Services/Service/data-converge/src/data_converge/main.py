"""Command-line argument, output and process error boundary."""
from __future__ import annotations

from contextlib import ExitStack
import json
import logging
import os

from data_converge.application import Application, CommandResult
from data_converge.config.application import (
    DEFAULT_APP_CONFIG_PATH, StartupError, load_app_config, load_definition,
)
from data_converge.observability.logging import LOG_FORMAT, LoggingSession
from data_converge.observability.settings import LoggingSettings
from data_converge.state.bootstrap import StateBootstrapError
from data_converge.diagnostics import cli as diagnostics
from data_converge.errors import safe_error_details
from data_converge.cli_help import add_operation_arguments, create_parser
from data_converge.configuration_migration import cli as configuration_migration
from data_converge.backfill import cli as backfill

logger = logging.getLogger(__name__)


def arguments(argv):
    parser = create_parser()
    modes = parser.add_mutually_exclusive_group()
    diagnostics.add_arguments(parser, modes)
    add_operation_arguments(parser, modes)
    configuration_migration.add_arguments(parser, modes)
    backfill.add_arguments(parser, modes)
    args = parser.parse_args(argv)
    backfill.validate_arguments(parser, args)
    diagnostics.validate_arguments(parser, args)
    configuration_migration.validate_arguments(parser, args)
    if args.start_time is not None and not args.init_state:
        parser.error('--start-time requires --init-state')
    if args.now is not None and not args.run_once:
        parser.error('--now requires --run-once')
    if args.run_task and not args.scheduled_time:
        parser.error('--run-task requires --scheduled-time')
    if args.scheduled_time is not None and not args.run_task:
        parser.error('--scheduled-time requires --run-task')
    if (args.resolution is not None or args.note is not None) and not args.resolve_instance:
        parser.error('--resolution and --note require --resolve-instance')
    if args.resolve_instance and (not args.resolution or not args.note):
        parser.error('--resolve-instance requires --resolution and --note')
    return args


def output(value):
    print(json.dumps(value, ensure_ascii=False, default=str))


def run(args, *, raw_config=None) -> CommandResult:
    application = Application(args.config, raw_config=raw_config)
    if args.apply_config is not None:
        return CommandResult(payload=configuration_migration.apply(args, application))
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
    if backfill.is_command(args):
        return backfill.run(args)
    if diagnostics.is_diagnostic(args):
        return diagnostics.run(args)
    stage = 'LOGGING'
    with ExitStack() as cleanup:
        try:
            logs = cleanup.enter_context(LoggingSession())
            stage = 'CONFIG'
            config = load_app_config(args.config)
            stage = 'LOGGING'
            logs.configure(LoggingSettings.from_mapping(config.get('logging')))
            stage = 'APPLICATION'
            result = run(args, raw_config=config)
            if result.payload is not None:
                output(result.payload)
            return result.exit_code
        except (Exception, KeyboardInterrupt) as exc:
            # Log before cleanup; untrusted SDK/server/SQL bodies stay private.
            detail = str(exc) if isinstance(exc, (StartupError, StateBootstrapError)) else type(exc).__name__
            logger.error('SERVICE_FAILED: %s stage=%s diagnostic=%s', detail, stage,
                         json.dumps(safe_error_details(exc), ensure_ascii=False))
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
