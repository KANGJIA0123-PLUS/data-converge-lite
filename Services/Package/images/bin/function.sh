#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
umask 0027
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

#初始化环境变量
function init_env() {
    set +e
    source /etc/profile
    set -e
    CURRENT_PATH=$(pwd)
    LOG_PATH="/var/log/its/default_service"
    LOG_PATH=$(get_config "LOG_PATH")
    LOG_NAME=${LOG_NAME-default_service.log}
    LOG_FILE="${LOG_PATH}/${LOG_NAME}"
    SERVICE_NAME=$(get_config "SERVICE_NAME")
    log_archive "${LOG_FILE}"
}

#获取服务配置项
function get_config() {
    local config_key=$1
    local nullable=${2:-'false'}
    local config_file="${CURRENT_PATH}/service.conf"
    local value env_name
    if [[ -f ${config_file} ]]; then
        value=$(grep "^${config_key}=" "${config_file}" | sed 's#[[:blank:]]*=[[:blank:]]*#=#' | sed "s#^${config_key}=##")
    fi
    if [[ -z ${value} ]]; then
        if [[ ${nullable} == "false" ]]; then
            log "error" "Can not find value of ${config_key} from ${config_file}"
            exit_error
        else
            echo ""
            return
        fi
    fi

    #安全解析变量
    for i in $(echo "${value}" | grep -Eo '\$\{[a-zA-Z0-9_-]*\}'); do
        env_name=$(echo ${i} | sed 's/[\{,\,\$}]//g')
        value=$(echo ${value} | sed 's#\${'${env_name}'}#'${!env_name}'#g')
    done
    echo "${value}"
}

#检查日志文件路径及文件
function check_log_file() {
    if [[ ! -d ${LOG_PATH} ]]; then
        mkdir -p "${LOG_PATH}"
        chmod 700 "${LOG_PATH}"
    fi
    if [[ ! -f ${LOG_FILE} ]]; then
        touch "${LOG_FILE}"
        chmod 600 "${LOG_FILE}"
    fi
}

#记录日志
function log() {
    #需要公共变量:LOG_FILE
    check_log_file
    #拆分过大日志
    log_archive "${LOG_FILE}"

    shell_name=$(echo "$0" | awk -F '/' '{print $NF}')
    pid_num=$$
    status=$(echo $1 | sed 's/[a-z]/\U&/g')
    message=$2
    executor=$(whoami)
    echo "[$(date)]|${shell_name}|${pid_num}|${executor}|${status}|${message}"
    echo "[$(date)]|${shell_name}|${pid_num}|${executor}|${status}|${message}" >>"${LOG_FILE}"
}

#拆分日志文件
function log_archive() {
    local log_file=$1
    local run_size
    if [[ ! -f ${log_file} ]]; then return; fi
    run_size=$(du -b "${log_file}" | awk '{print $1}')
    local size_of_10m=$((10 * 1024 * 1024))

    if [[ "${run_size}" -gt "${size_of_10m}" ]]; then
        #日志保留最大个数
        local log_num=9
        local tmp_old_num=0
        local log_name
        log_name=$(echo "${log_file}" | awk -F '/' '{print $NF}')
        local log_filepath
        log_filepath=$(dirname "${log_file}")

        cd "${log_filepath}" || exit 1
        for num in $(ls | grep "${log_name}_.*.tar.gz" | grep -Po '\d+(?=.tar.gz)' | sort -n -r); do
            if [ "$num" -ge "$log_num" ]; then
                rm -f "${log_name}_${num}.tar.gz"
                continue
            fi
            tmp_old_num=${num}
            num=$((num + 1))
            mv "${log_name}_${tmp_old_num}.tar.gz" "${log_name}_${num}.tar.gz"
            chmod 400 "${log_name}_${num}.tar.gz"
        done
        tar zcvf "${log_name}_1.tar.gz" "${log_name}"
        chmod 400 "${log_name}_1.tar.gz"
        echo "" >"${log_file}"
        cd - || exit 1
    fi
}

#显示Title
function show_title() {
    local content=$1
    check_log_file
    cat <<EOF | tee -a "${LOG_FILE}"
============================================================
                    ${content}
============================================================
EOF
}

#错误退出
function exit_error() {
    #退出延迟时间
    local delay_seconds=$1
    log "error" "Error occurred during execute script, exit with error"
    #如果有入参则延迟退出
    if [[ -n ${delay_seconds} ]] && [[ ${delay_seconds} -gt 0 ]]; then
        log "error" "Exit delay time : ${delay_seconds}s"
        sleep "${delay_seconds}"
    fi
    exit 1
}

#检查普通返回值是否为0
function check_result() {
    local result=$1
    local delay_seconds=$2
    if [[ ${result} -ne 0 ]]; then
        log "error" "Result check not equal 0"
        exit_error "${delay_seconds}"
    fi
}

#校验是否为true
function is_true() {
    local input_str
    input_str=$(echo "$1" | sed 's/[a-z]/\U&/g')
    if [[ "${input_str}" == "TRUE" ]]; then
        return 0
    else
        return 1
    fi
}

#调用自定义脚本
function call_custom_script() {
    local custom_script=$1
    if [[ -f "${custom_script}" ]]; then
        log "info" "Find custom script ${custom_script}, begin to call"
        sh "${custom_script}" >>"${LOG_FILE}"
        check_result $?
    fi
}

#服务状态汇总
function service_status_summary() {
    local status=$1
    local message=$2
    local status_summary_file=${LOG_PATH}/service_status_summary.log
    local current_time
    current_time=$(date '+%Y-%m-%d %H:%M:%S')
    if [[ ! -f ${status_summary_file} ]]; then
        {
            echo "last_start_time="
            echo "last_status="
            echo "last_check_time="
            echo "last_check_success_time="
            echo "last_check_fail_time="
            echo "last_check_fail_message="
        } >"${status_summary_file}"
    fi

    sed -i "s/last_check_time=.*/last_check_time=${current_time}/g" "${status_summary_file}"
    if [[ ${status} == "success" ]]; then
        sed -i "s/last_status=.*/last_status=${status}/g" "${status_summary_file}"
        sed -i "s/last_check_success_time=.*/last_check_success_time=${current_time}/g" "${status_summary_file}"
    elif [[ ${status} == "fail" ]]; then
        sed -i "s/last_status=.*/last_status=${status}/g" "${status_summary_file}"
        sed -i "s/last_check_fail_time=.*/last_check_fail_time=${current_time}/g" "${status_summary_file}"
        if [[ -n ${message} ]]; then
            sed -i "s/last_check_fail_message=.*/last_check_fail_message=${message}/g" "${status_summary_file}"
        fi
    elif [[ ${status} == "start" ]]; then
        sed -i "s/last_start_time=.*/last_start_time=${current_time}/g" "${status_summary_file}"
    else
        log "error" "Check input service status failed, please check"
        exit_error
    fi
}

#检查环境变量依赖
function check_env_dependency() {
    local env_key=$1
    if [[ -z $(env | grep "${env_key}") ]]; then
        log "error" "Can not find env dependency of ${env_key}"
        return 1
    fi
    return 0
}

#检查文件依赖
function check_file_dependency() {
    local file=$1
    if [[ -f ${file} ]]; then
        log "error" "Can not find file dependency of ${file}"
        return 1
    fi
    return 0
}

#服务启动前依赖检查
function pre_check_service() {
    local env_dependency file_dependency
    env_dependency=($(get_config "ENV_DEPENDENCY" | tr "," " "))
    file_dependency=($(get_config "FILE_DEPENDENCY" | tr "," " "))

    for i in "${env_dependency[@]}"; do
        check_env_dependency "${i}"
        check_result $?
    done

    for j in "${file_dependency[@]}"; do
        check_file_dependency "${j}"
        check_result $?
    done
}

#初始化python环境
function init_python() {
    log "info" "Start init python"
    local use_python load_site_packages
    use_python=$(get_config "USE_PYTHON" "true")
    if is_true "${use_python}"; then
        log "info" "USE_PYTHON is true, loading python env"
        cd /opt/gde/python/python-3.* || exit_error
        source python.env
        cd - || exit_error
    else
        log "info" "USE_PYTHON is not true, abort loading python env"
        return
    fi

    load_site_packages=$(get_config "LOAD_SITE_PACKAGES")
    if is_true "${load_site_packages}"; then
        log "info" "LOAD_SITE_PACKAGES is true, loading python site-packages"
        if [[ ! -f ${CURRENT_PATH}/py/python_pkg.tar.gz ]]; then
            log "error" "Can not find ${CURRENT_PATH}/py/python_pkg.tar.gz, please check"
            exit_error
        fi
        log "info" "Start to decompress python_pkg.tar.gz"
        cd "${CURRENT_PATH}/py" || exit_error
        tar -xf "python_pkg.tar.gz"
        check_result $?
        chmod 750 -R site-packages
        cd - || exit_error
        log "info" "Decompress python_pkg.tar.gz done"
        log "info" "Load decompressed site-packages env"
        export PYTHONPATH=${PYTHONPATH}:${CURRENT_PATH}/py/site-packages
    else
        log "info" "LOAD_SITE_PACKAGES is not true, abort loading python site-packages"
    fi
}

#启动java进程
function start_java() {
    log "info" "Starting java process"
    set +e
    source /etc/profile
    set -e
    java_bin=$(command -v java)

    local startup_mode jvm_opts main_class classpath jar_file
    startup_mode=$(get_config "STARTUP_MODE")
    jvm_opts=$(get_config "JVM_OPTS")

    log "info" "Current startup mode is ${startup_mode}"
    if [[ ${startup_mode} == "class" ]]; then
        main_class=$(get_config "MAIN_CLASS")
        classpath=$(get_config "CLASSPATH")
        java_para="${jvm_opts} -classpath ${classpath} ${main_class}"
    elif [[ ${startup_mode} == "jar" ]]; then
        jar_file=$(get_config "JAR_FILE")
        #支持JAR_FILE配置中包含通配符
        jar_file=$(ls ${jar_file})
        java_para="-jar ${jvm_opts} ${jar_file}"
    else
        log "error" "Can not find proper startup_mode: ${startup_mode}"
        exit_error
    fi

    log "info" "Java command: ${java_bin} ${java_para}"
    # shellcheck disable=SC2086
    # nohup参数需要单独接收，不能拼接成完整字符串，重定向为了保留java启动时的错误信息
    nohup "${java_bin}" ${java_para} >>"${LOG_FILE}" 2>&1 &
    local pid=$!
    log "info" "Start service with pid: ${pid}"
    echo "${pid}" >"${CURRENT_PATH}/../config/.pid"
    chmod 600 "${CURRENT_PATH}/../config/.pid"
}

#执行Python二次开发脚本
function run_python_lifecycle_script() {
    local script_name=$1
    local script_param=${2:-}
    log "info" "Init custom site-packages path"
    cd "${CURRENT_PATH}/../scripts/"
    if [[ $(arch) == "x86_64" ]]; then
        dependency_name=dependency_x86
    else
        dependency_name=dependency_arm
    fi
    tar xf ${dependency_name}.tar.gz

    log "info" "Load python env"
    cd /opt/gde/python/python-3.* || exit_error
    source python.env
    export PYTHONPATH="${CURRENT_PATH}/../scripts/${dependency_name}":"${CURRENT_PATH}/../scripts/${dependency_name}/gaussdb_client":${PYTHONPATH}
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:"${CURRENT_PATH}/../scripts/${dependency_name}/gaussdb_client"
    export LOG_PATH
    cd - || exit_error

    script_path="${CURRENT_PATH}/../scripts/${script_name}"
    if [[ -f ${script_path} ]]; then
        log "info" "Executing python script ${script_name}..."
        python3 "${script_path}" "${script_param}"
        check_result $?
    else
        log "info" "Python script ${script_name} not found, skipping..."
    fi
}

init_env
