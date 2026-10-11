#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1
LOG_NAME="post-start.log"
source "./function.sh"

show_title "post-start@${SERVICE_NAME}"

exit 0
