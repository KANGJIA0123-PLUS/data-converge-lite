#!/usr/bin/env bash
# Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "${script_dir}" || exit 1
LOG_NAME="start.log"
source "${script_dir}/function.sh"
source "${script_dir}/start_data_converge.sh"
export SERVICE_HOME="${SERVICE_HOME:-/opt/its/${SERVICE_NAME}}"

# Keep the company's startup permissions and lifecycle checks in this entrypoint.
function config_permission() {
    log "info" "Start to config permission of common files"
    {
        chmod -v 500 "${SERVICE_HOME}/bin"
        chmod -v 500 "${SERVICE_HOME}/lib"
        chmod -v 500 "${SERVICE_HOME}/bin/"*.sh
    } >>"${LOG_FILE}"
}

function config_python() {
    log "info" "Start to config permission of python dependencies"
    local depen_pkg="${SERVICE_HOME}/bin/py"
    find "${depen_pkg}/" -type d -exec chmod 700 {} \;
    find "${depen_pkg}/" -type f -exec chmod 500 {} \;
    find "${depen_pkg}/" -name "*.pem" -exec chmod 400 {} \;
    find "${depen_pkg}/" -name "*.crt" -exec chmod 400 {} \;
    find "${depen_pkg}/" -name "*.key" -exec chmod 400 {} \;
    log "info" "Python dependency permissions configured"
}

function start_service() {
    pre_check_service
    init_python
    config_python
    service_status_summary "start"
    check_result $?
    log "info" "Starting data-converge, for more information please check ${LOG_PATH}"
    # The business launcher execs Python. Do not leave a sleeping shell behind
    # when the application exits, or log startup success before it is ready.
    start_data_converge "$@"
}

function main() {
    show_title "start@${SERVICE_NAME}"
    config_permission
    start_service "$@"
}

main "$@"
