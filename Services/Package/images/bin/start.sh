#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
LOG_NAME="start.log"
source "./function.sh"
source "./command_env.sh"
source start_data_converge.sh
#配置权限
function config_permission() {
    log "info" "Start to config permission of common files"
    {
        chmod -v 500 "/opt/its/${SERVICE_NAME}/bin"
        chmod -v 500 "/opt/its/${SERVICE_NAME}/lib"
        chmod -v 500 "/opt/its/${SERVICE_NAME}/bin/"*.sh
        chmod -v 500 "/opt/its/${SERVICE_NAME}/bin/dconv"
    } >>"${LOG_FILE}"
}

function config_python(){
    log "info" "Start init python"

    GDE_PYTHON_PATH=$(find /opt/gde/python -maxdepth 1 -type d -name "python-3.*")
    DEPEN_PKG=/opt/its/${SERVICE_NAME}/bin/py

    cd ${GDE_PYTHON_PATH} || exit_error
    source ./python.env
    cd - || exit_error
    echo "#################"

    CURRENT_PATH=/opt/its/${SERVICE_NAME}/bin
    # 处理 wheel 包
    unzip -o ${CURRENT_PATH}/pip-*.whl -d /tmp/pip_extracted
    SITE_PACKAGES=/opt/its/${SERVICE_NAME}/bin/py/site-packages
    mkdir -p ${SITE_PACKAGES}

    cp -r /tmp/pip_extracted/pip* ${SITE_PACKAGES}/
    [ -d /tmp/pip_extracted ] && rm -rf /tmp/pip_extracted
    cd - || exit_error

    log "info" "Decompress python_pkg.tar.gz done"
    log "info" "Load decompressed site-packages env"
    export PYTHONPATH=${SITE_PACKAGES}:${DEPEN_PKG}:${PYTHONPATH}
    pip3.12 --version

    cd ${DEPEN_PKG}
    find ${DEPEN_PKG}/ -type d -exec chmod 700 {} \;
    find ${DEPEN_PKG}/ -type f -exec chmod 500 {} \;
    find ${DEPEN_PKG}/ -name "*.pem" -exec chmod 400 {} \;
    find ${DEPEN_PKG}/ -name "*.crt" -exec chmod 400 {} \;
    find ${DEPEN_PKG}/ -name "*.key" -exec chmod 400 {} \;
    cd - || exit_error
    log "info" "start_python!!!"
}

#启动服务
function start_service() {
    pre_check_service
    init_python
    config_python
    service_status_summary "start"
    check_result $?
    start_data_converge "$@"

    log "info" "Start service success, for more information please check ${LOG_PATH}"
    wait_data_converge
}

function main() {
    show_title "start@${SERVICE_NAME}"
    config_permission
    if bash ./register_command.sh >>"${LOG_FILE}" 2>&1; then
        log "info" "COMMAND_ENV_REGISTERED: dconv available in new Bash sessions for this account"
    else
        log "warn" "COMMAND_ENV_REGISTRATION_FAILED: service startup continues; use bash /opt/its/${SERVICE_NAME}/bin/dconv"
    fi
    start_service "$@"
}

#代码入口
main "$@"

exit 0
