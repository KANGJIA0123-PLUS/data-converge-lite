#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import json
import os
import sys
from importlib.util import module_from_spec
from importlib.util import spec_from_file_location
from random import randint

from its_api.configmap import get_configmap, create_configmap, update_configmap, check_configmap_exist
from its_api.logger import LOGGER

# 动态导入本地文件
current_path = os.path.split(__file__)[0]
upgrade_util_path = os.path.join(current_path, 'upgrade_util.py')

upgrade_util_spec = spec_from_file_location(f'upgrade_util_{randint(0, 999999)}', upgrade_util_path)
upgrade_util = module_from_spec(upgrade_util_spec)
upgrade_util_spec.loader.exec_module(upgrade_util)

# 导入upgrade_util模块
get_pending_version_list = upgrade_util.get_pending_version_list
gkit_interface = upgrade_util.gkit_interface
revoke_custom_sub_method = upgrade_util.revoke_custom_sub_method
revoke_custom_upgrade_method = upgrade_util.revoke_custom_upgrade_method
execute_sql = upgrade_util.execute_sql
get_upgrade_params = upgrade_util.get_upgrade_params

# 设定日志文件
log_path = os.getenv("LOG_PATH")
LOGGER.set_logger(os.path.join(log_path, "rollback.log"))


@gkit_interface
def pre_rollback(service_path, version=None):
    """
    GKIT回退接口：回退前置处理

    :param service_path: 服务包路径
    :param version: 版本信息
    :return: True/False
    """
    # 各服务自定义处理接口
    if not revoke_custom_upgrade_method("custom_pre_rollback", service_path, version):
        return False
    rollback_version_list = get_pending_version_list(service_path, version, "rollback")
    LOGGER.info("Find version list to rollback: %s", str(rollback_version_list))
    for sub_version in rollback_version_list:
        # 执行各个子版本sub_rollback.pre_dbrollback
        result = revoke_custom_sub_method("rollback", sub_version, "pre_dbrollback", service_path,
                                          version)
        if result:
            LOGGER.info("Succeed to execute pre_dbrollback of %s", sub_version)
        else:
            LOGGER.error("Failed to execute pre_dbrollback of %s", sub_version)
            return False
    return True


@gkit_interface
def post_rollback(service_path, version=None):
    """
    GKIT回退接口：回退操作

    :param service_path: 服务包路径
    :param version: 版本信息
    :return: True/False
    """
    # 获取各个子版本并升序排序
    rollback_version_list = get_pending_version_list(service_path, version, "rollback")
    rollback_path = os.path.join(service_path, 'rollback')
    LOGGER.info("Find version list to rollback: %s", str(rollback_version_list))

    for sub_version in rollback_version_list:
        sub_path = os.path.join(rollback_path, sub_version)
        # 执行高斯库SQL
        if os.path.exists(sub_path) and 'gssql' in os.listdir(sub_path):
            sql_path = os.path.join(sub_path, 'gssql')
            if not execute_sql(sql_path):
                return False

        # 执行各个子版本sub_rollback.post_dbrollback
        result = revoke_custom_sub_method("rollback", sub_version, "post_dbrollback", service_path,
                                          version)
        if result:
            LOGGER.info("Succeed to execute post_dbrollback of %s", sub_version)
        else:
            LOGGER.error("Failed to execute post_dbrollback of %s", sub_version)
            return False
    return rollback_version(version)


def rollback_version(version=None):
    target_version = version.get("target_version")
    configmap_name = 'its-version'
    if not check_configmap_exist(configmap_name):
        LOGGER.info("Config map %s is not exist", configmap_name)
        return False
    config_data = get_configmap(
        configmap_name=configmap_name, namespace=os.getenv("PROJECT_NAME")).get('data')
    if not config_data:
        LOGGER.error(f"Config map {configmap_name} config_data is empty")
        return False
    upgrade_chain = config_data.get("UPGRADE_CHAIN")
    if not upgrade_chain:
        LOGGER.error(f"Config map {configmap_name} UPGRADE_CHAIN is None")
        return False
    upgrade_chains = json.loads(upgrade_chain)
    if target_version in upgrade_chains and len(upgrade_chains) > 1:
        upgrade_chains.remove(target_version)
    else:
        return True
    config_data["UPGRADE_CHAIN"] = json.dumps(upgrade_chains)
    return update_configmap(configmap_name, config_data)


if __name__ == '__main__':
    cnai_upgrade_mode = os.environ.get('CNAI_UPGRADE_MODE', '')
    LOGGER.info(f'cnai_upgrade_mode is: {cnai_upgrade_mode}')
    if cnai_upgrade_mode and cnai_upgrade_mode != 'normal,upgrade':
        LOGGER.info('It is not normal rollback scene. do nothing.')
        sys.exit(0)
    service_path, version_info, lifecycle = get_upgrade_params()
    # 执行升级任务
    if lifecycle == "post_rollback":
        if post_rollback(service_path, version_info):
            sys.exit(0)
        else:
            sys.exit(1)
    elif lifecycle == "pre_rollback":
        if pre_rollback(service_path, version_info):
            sys.exit(0)
        else:
            sys.exit(1)
