#!/usr/bin/env bash
# Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.

# Shared by the foreground entrypoint and both probes. The optional argument
# controls dependency installation; probes never mutate the deployed tree.
function configure_data_converge_runtime() {
    local install_dependencies=${1:-false}
    local env_file previous_dir dependency_name dependency_dir site_dir candidate
    local env_candidates=()
    export SERVICE_HOME="${SERVICE_HOME:-/opt/its/${SERVICE_NAME:-data-converge}}"
    env_file=${PYTHON_ENV_FILE:-}
    if [[ -z "${env_file}" ]]; then
        for candidate in /opt/gde/python/python-3.*/python.env; do
            [[ -f "${candidate}" ]] && env_candidates+=("${candidate}")
        done
        if [[ ${#env_candidates[@]} -ne 1 ]]; then
            echo "Set PYTHON_ENV_FILE to the GDE python.env (expected one installed version)." >&2
            return 1
        fi
        env_file=${env_candidates[0]}
    fi
    if [[ ! -f "${env_file}" ]]; then
        echo "Python environment file does not exist: ${env_file}" >&2
        return 1
    fi
    previous_dir=$PWD
    cd "$(dirname "${env_file}")" || return 1
    source "./$(basename "${env_file}")"
    cd "${previous_dir}" || return 1

    case "$(uname -m)" in
        x86_64|amd64) dependency_name=dependency_x86 ;;
        aarch64|arm64) dependency_name=dependency_arm ;;
        *) echo "Unsupported machine architecture: $(uname -m)" >&2; return 1 ;;
    esac
    site_dir="${SERVICE_HOME}/bin/py/site-packages"
    dependency_dir="${SERVICE_HOME}/scripts/${dependency_name}"
    if [[ "${install_dependencies}" == true ]]; then
        if [[ ! -d "${site_dir}" && -f "${SERVICE_HOME}/bin/py/python_pkg.tar.gz" ]]; then
            tar -xzf "${SERVICE_HOME}/bin/py/python_pkg.tar.gz" -C "${SERVICE_HOME}/bin/py"
            [[ -d "${site_dir}" ]] || { echo "Dependency archive lacks site-packages." >&2; return 1; }
        fi
        if [[ ! -d "${dependency_dir}" && -f "${dependency_dir}.tar.gz" ]]; then
            tar -xzf "${dependency_dir}.tar.gz" -C "${SERVICE_HOME}/scripts"
            [[ -d "${dependency_dir}" ]] || { echo "Dependency archive lacks ${dependency_name}." >&2; return 1; }
        fi
    fi
    export PYTHONPATH="${SERVICE_HOME}/lib/data-converge/src:${site_dir}${PYTHONPATH:+:${PYTHONPATH}}"
    if [[ -d "${dependency_dir}" ]]; then
        export PYTHONPATH="${PYTHONPATH}:${dependency_dir}:${dependency_dir}/gaussdb_client"
        export LD_LIBRARY_PATH="${dependency_dir}/gaussdb_client${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    fi
    export PYTHONDONTWRITEBYTECODE=1
    export APP_CONF_FILE="${APP_CONF_FILE:-${SERVICE_HOME}/lib/data-converge/resources/config/app_config.yaml}"
}

function start_data_converge() {
    configure_data_converge_runtime true
    export LOG_PATH SERVICE_NAME PRODUCT_NAME
    DATA_CONVERGE_PID=
    DATA_CONVERGE_WAIT_INTERRUPTED=false
    # Bash background commands inherit ignored SIGINT. Both signals request
    # the application's graceful shutdown through SIGTERM instead.
    trap 'stop_data_converge' TERM INT
    python3 -m data_converge.main --config "${APP_CONF_FILE}" "$@" &
    DATA_CONVERGE_PID=$!
    # A signal may arrive between installing the trap and recording the PID.
    if [[ "${DATA_CONVERGE_WAIT_INTERRUPTED}" == true ]]; then
        stop_data_converge
    fi
}

function stop_data_converge() {
    DATA_CONVERGE_WAIT_INTERRUPTED=true
    if [[ -n "${DATA_CONVERGE_PID:-}" ]]; then
        kill -TERM "${DATA_CONVERGE_PID}" 2>/dev/null || :
    fi
}

function wait_data_converge() {
    local exit_code
    while true; do
        DATA_CONVERGE_WAIT_INTERRUPTED=false
        exit_code=0
        wait "${DATA_CONVERGE_PID}" || exit_code=$?
        # A trapped signal interrupts wait before Python finishes cleanup.
        [[ "${DATA_CONVERGE_WAIT_INTERRUPTED}" == false ]] && break
    done
    trap - TERM INT
    unset DATA_CONVERGE_PID DATA_CONVERGE_WAIT_INTERRUPTED
    return "${exit_code}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    set -e
    start_data_converge "$@"
    wait_data_converge
fi
