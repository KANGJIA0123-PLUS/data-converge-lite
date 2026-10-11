#!/usr/bin/env bash
# Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repository_path=${REPOSITORY_PATH:-$(cd "${script_dir}/../../.." && pwd)}
exec "${PYTHON_BIN:-python3}" "${repository_path}/tools/build_package.py" "$@"
