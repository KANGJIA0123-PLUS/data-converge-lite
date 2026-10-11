#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
LOG_NAME="post-rollback.log"
source "./function.sh"

# 执行Python二次开发脚本
show_title "post-rollback@${SERVICE_NAME}"
run_python_lifecycle_script "rollback.py" "post_rollback"

exit $?
