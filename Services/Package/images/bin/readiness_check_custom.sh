#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.

set -e

source "./function.sh"

function do_service_check() {
        cat /tmp/$1 && rm -f /tmp/$1
        if [[  ! $? -eq 0  ]]; then
            log "error" "$1 is no exits"
            exit 2
        fi
}

do_service_check ready_tag
