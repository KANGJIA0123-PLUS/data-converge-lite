#!/usr/bin/env bash
# Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LOG_NAME="start.log"
# Keep the platform's environment and logging initialization. Its init_python
# unconditionally unpacks/chmods dependencies, so use the shared runtime below.
source "${script_dir}/function.sh"
source "${script_dir}/start_data_converge.sh"
start_data_converge "$@"
