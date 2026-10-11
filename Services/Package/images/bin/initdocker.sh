#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
CURRENT_PATH=$(pwd)
USER_NAME=modelmate
USER_ID=19101
GROUP_NAME=modelmate
GROUP_ID=19101

service_name=$1

#记录日志
function log() {
    shell_name=$(echo "$0" | awk -F '/' '{print $NF}')
    pid_num=$$
    status=$(echo $1 | sed 's/[a-z]/\U&/g')
    message=$2
    executor=$(whoami)
    echo "[$(date)]|${shell_name}|${pid_num}|${executor}|${status}|${message}"
}

#调用自定义脚本
function call_custom_script() {
    local custom_script=$1
    if [[ -f "${custom_script}" ]]; then
        log "info" "Find custom script ${custom_script}, begin to call"
        sh "${custom_script}"
        if [[ $? -ne 0 ]]; then
            log "error" "Error occurred during execute ${custom_script}, please check"
            exit 1
        fi
    fi
}

#添加sudo配置
function add_sudo_config() {
    log "info" "Copy sudo files"
    install -o root -g root -m 0600 -d /var/scripts
    mv -v "/opt/its/${service_name}/bin/sudo_init.sh" /var/scripts/
    if [ -f "/opt/its/${service_name}/bin/sudo_init_custom.sh" ]; then
        mv -v "/opt/its/${service_name}/bin/sudo_init_custom.sh" /var/scripts/
    fi
    chmod 500 /var/scripts/*.sh
    chown root:root /var/scripts/*.sh
}

#配置用户和组
function config_user_group() {
    log "info" "Start to add os user ${USER_NAME}:${USER_ID}, group ${GROUP_NAME}:${GROUP_ID}"
    groupadd -g ${GROUP_ID} ${GROUP_NAME}
    useradd -g ${GROUP_NAME} -d /home/${USER_NAME} -m -u ${USER_ID} -s /bin/bash ${USER_NAME}
    usermod -G common ${USER_NAME}
}

#创建路径
function create_dir() {
    log "info" "Create workdir /opt/its/${service_name}"
    mkdir -p "/opt/its"
    chmod 700 "/opt/its"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its"

    mkdir -p "/opt/its/${service_name}"
    chmod 700 "/opt/its/${service_name}"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/${service_name}"

    log "info" "Create configmap dir /opt/its/configmap"
    mkdir -p "/opt/its/configmap"
    chmod 700 "/opt/its/configmap"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/configmap"

    log "info" "Create platform dir /opt/its/platform"
    mkdir -p "/opt/its/platform"
    chmod 700 "/opt/its/platform"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/platform"

    log "info" "Create secret dir /opt/its/secret"
    mkdir -p "/opt/its/secret"
    chmod 700 "/opt/its/secret"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/secret"

    log "info" "Create thirdparty dir /opt/its/thirdparty"
    mkdir -p "/opt/its/thirdparty"
    chmod 700 "/opt/its/thirdparty"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/thirdparty"

    log "info" "Create ${service_name} configmap dir /opt/its/${service_name}/configmap"
    mkdir -p "/opt/its/${service_name}/configmap"
    chmod 700 "/opt/its/${service_name}/configmap"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/${service_name}/configmap"
    mkdir -p "/opt/its/${service_name}/configmap/its-common-cfg-configmap"
    chmod 700 "/opt/its/${service_name}/configmap/its-common-cfg-configmap"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/${service_name}/configmap/its-common-cfg-configmap"


    log "info" "Create ${service_name} secret dir /opt/its/${service_name}/secret"
    mkdir -p "/opt/its/${service_name}/secret"
    chmod 700 "/opt/its/${service_name}/secret"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/${service_name}/secret"
    mkdir -p "/opt/its/${service_name}/secret/its-common-secret"
    chmod 700 "/opt/its/${service_name}/secret/its-common-secret"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/${service_name}/secret/its-common-secret"

    log "info" "Create ${service_name} config dir /opt/its/${service_name}/config"
    mkdir -p "/opt/its/${service_name}/config"
    chmod 700 "/opt/its/${service_name}/config"
    chown ${USER_NAME}:${GROUP_NAME} "/opt/its/${service_name}/config"

    log "info" "Create ${service_name} srv dir /srv/its/MindOpsBOSE/${service_name}"
    mkdir -p "/srv/its/MindOpsBOSE/${service_name}"
    chmod -R 750 /srv/its
    chown -R ${USER_NAME}:${GROUP_NAME} /srv/its
}

#删除初始化脚本
function clear_init_script() {
    log "info" "Start to clear init script"
    rm -vf "${CURRENT_PATH}/initdocker.sh"
    rm -vf "${CURRENT_PATH}/initdocker_custom.sh"
}

#配置时区
function config_timezone() {
    #解决夏令时问题，时区数据跟节点同步
    if [[ -e /etc/localtime ]]; then
        rm -fv /etc/localtime
    fi
}

function main() {
    log "info" "Start to execute init docker script"
    config_user_group
    create_dir
    add_sudo_config
    config_timezone
    call_custom_script "${CURRENT_PATH}/initdocker_custom.sh"
    clear_init_script
}

#代码入口
main

exit 0
