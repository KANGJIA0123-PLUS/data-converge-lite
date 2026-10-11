#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
CURRENT_PATH=$(pwd)
USER=modelmate
GROUP=modelmate
SERVICE_NAME=data-converge
PRODUCT_NAME=MindOpsBOSE
LOG_FILE="/srv/its/MindOpsBOSE/data-converge/log/sudo_init.log"

#初始化log路径
function init_log() {
    log_path=$(dirname ${LOG_FILE})
    mkdir -p "${log_path}"
    chmod 700 "${log_path}"
    chown ${USER}:common "${log_path}"
}

#记录日志
function log() {
    shell_name=$(echo "$0" | awk -F '/' '{print $NF}')
    pid_num=$$
    status=$(echo $1 | sed 's/[a-z]/\U&/g')
    message=$2
    executor=$(whoami)
    echo "[$(date)]|${shell_name}|${pid_num}|${executor}|${status}|${message}"
    echo "[$(date)]|${shell_name}|${pid_num}|${executor}|${status}|${message}" >>"${LOG_FILE}"
}

#调用自定义脚本
function call_custom_script() {
    local custom_script=$1
    if [[ -f "${custom_script}" ]]; then
        log "info" "Find custom script ${custom_script}, begin to call"
        sh "${custom_script}" >>"${LOG_FILE}"
        if [[ $? -ne 0 ]]; then
            log "error" "Error occurred during execute custom sudo script, please check"
            exit 1
        fi
    fi
}

#清理sudo配置
function remove_sudo_config() {
    log "info" "Remove sudo config"
    if [[ -f "${CURRENT_PATH}/sudo_init.sh" ]]; then
        rm -fv "${CURRENT_PATH}/sudo_init.sh" >>"${LOG_FILE}"
    fi
    if [[ -f "${CURRENT_PATH}/sudo_init_custom.sh" ]]; then
        rm -fv "${CURRENT_PATH}/sudo_init_custom.sh" >>"${LOG_FILE}"
    fi
}

#配置挂载路径权限
function config_mount_path() {
    log "info" "Start to config permission of mount path"
    local run_log_path=/srv/its/MindOpsBOSE/data-converge/log
    local audit_log_path=/var/paas/sys/log/its/
    local data_path=/srv/its/MindOpsBOSE/data-converge/data
    local temp_path=/srv/its/MindOpsBOSE/data-converge/temp

    {
        chmod -v 750 ${run_log_path}
        chown -v ${USER}:common ${run_log_path}
        chmod -v 750 ${audit_log_path}
        chown -v ${USER}:${GROUP} ${audit_log_path}
        chmod -v 750 ${data_path}
        chown -v ${USER}:${GROUP} ${data_path}
        chmod -v 750 ${temp_path}
        chown -v ${USER}:${GROUP} ${temp_path}
        chmod -v 750 "/srv/its"
        chmod -v 750 "/srv/its/${PRODUCT_NAME}"
        chmod -v 750 "/srv/its/${PRODUCT_NAME}/${SERVICE_NAME}"
        chmod -v 750 "/var/paas/sys/log/${PRODUCT_NAME}"
        chmod -v 750 "/var/paas/sys/log/${PRODUCT_NAME}/${SERVICE_NAME}"
        chmod -v 700 "/opt/its/configmap/its-common-cfg-configmap"
        chown -v ${USER}:${GROUP} "/opt/its/configmap/its-common-cfg-configmap"
        chmod -v 700 "/opt/its/secret/its-common-secret"
        chown -v ${USER}:${GROUP} "/opt/its/secret/its-common-secret"
    } >>"${LOG_FILE}"
}

function main() {
    init_log
    log "info" "Start to execute container sudo init script"
    config_mount_path
    call_custom_script "${CURRENT_PATH}/sudo_init_custom.sh"
    remove_sudo_config
}

#代码入口
main

exit 0
