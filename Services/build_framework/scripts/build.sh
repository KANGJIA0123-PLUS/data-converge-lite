#!/usr/bin/env bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e
source "${WORKSPACE}/BuildTool/Services/build_framework/scripts/function.sh"


function build_data_converge(){
    echo "come to build_data_converge"
    cd ${REPOSITORY_PATH}/
    python3 -m compileall -b -f ${REPOSITORY_PATH}/Services/Service/data-converge/
    find ${REPOSITORY_PATH}/Services/Service/data-converge/ -name "*.py" -type f -print0 | xargs -0 rm
    ls -l  ${REPOSITORY_PATH}/Services/Service/data-converge/
    mkdir -p ${REPOSITORY_PATH}/Services/Package/images/lib
    cp -rv ${REPOSITORY_PATH}/Services/Service/data-converge  ${REPOSITORY_PATH}/Services/Package/images/lib/
}


function main(){
  build_data_converge
}

main
