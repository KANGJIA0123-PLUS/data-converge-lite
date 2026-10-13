#!/usr/bin/env bash
exit 0
set -e
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "${script_dir}/start_data_converge.sh"
configure_data_converge_runtime false
exec python3 -m data_converge.main --config "${APP_CONF_FILE}" --healthcheck readiness "$@"
