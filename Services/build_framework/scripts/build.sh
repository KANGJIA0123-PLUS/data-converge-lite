#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
source "${WORKSPACE}/BuildTool/Services/build_framework/scripts/function.sh"


function build_data_converge(){
    local source_dir="${REPOSITORY_PATH:?}/Services/Service/data-converge"
    local image_lib="${REPOSITORY_PATH}/Services/Package/images/lib"
    echo "come to build_data_converge"
    cd "${REPOSITORY_PATH}/"
    # Keep tests in the source tree without compiling or deleting their sources.
    (
        cd "${source_dir}"
        python3 -m compileall -b -f -d "${source_dir}" \
            -x '(^|/)(test|tests|__pycache__|\.pytest_cache)(/|$)' ./
    )
    find "${source_dir}/" -type d \
        \( -name test -o -name tests -o -name __pycache__ -o -name .pytest_cache \) -prune \
        -o -type f -name "*.py" -exec rm {} +
    ls -l "${source_dir}/"
    mkdir -p "${image_lib}"
    cp -rv "${source_dir}" "${image_lib}/"
    # Also remove test artifacts left in the image directory by earlier builds.
    find "${image_lib}/data-converge" -type d \
        \( -name test -o -name tests -o -name __pycache__ -o -name .pytest_cache \) \
        -prune -exec rm -rf {} +
}


function main(){
  build_data_converge
}

main
