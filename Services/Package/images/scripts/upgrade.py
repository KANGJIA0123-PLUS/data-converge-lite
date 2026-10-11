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
upgrade_constant_path = os.path.join(current_path, 'upgrade_constant.py')
upgrade_util_path = os.path.join(current_path, 'upgrade_util.py')

upgrade_util_spec = spec_from_file_location(f'upgrade_util_{randint(0, 999999)}', upgrade_util_path)
upgrade_util = module_from_spec(upgrade_util_spec)
upgrade_util_spec.loader.exec_module(upgrade_util)

if os.path.isfile(upgrade_constant_path):
    upgrade_constant_spec = spec_from_file_location(f'upgrade_constant_{randint(0, 999999)}', upgrade_constant_path)
    upgrade_constant = module_from_spec(upgrade_constant_spec)
    upgrade_constant_spec.loader.exec_module(upgrade_constant)

# 导入upgrade_util模块
compare_versions = upgrade_util.compare_versions
get_pending_version_list = upgrade_util.get_pending_version_list
gkit_interface = upgrade_util.gkit_interface
gkit_interface_params = upgrade_util.gkit_interface_params
revoke_custom_sub_method = upgrade_util.revoke_custom_sub_method
revoke_custom_upgrade_method = upgrade_util.revoke_custom_upgrade_method
revoke_custom_params_method = upgrade_util.revoke_custom_params_method
execute_sql = upgrade_util.execute_sql
get_upgrade_params = upgrade_util.get_upgrade_params

log_path = os.getenv("LOG_PATH")
LOGGER.set_logger(os.path.join(log_path, "upgrade.log"))


@gkit_interface
def pre_check(service_path, version=None):
    """
    GKIT升级接口：升级前检查
    公共检查及调用业务定制检查

    :param service_path: 服务包路径
    :param version: 版本信息
    :return: True/False，[信息列表]
    """
    # 升级前后版本检查
    if not version_check(version):
        return False
    # 各服务自定义处理接口
    return True


@gkit_interface
def pre_upgrade(service_path, version=None):
    """
    GKIT升级接口：升级前置处理

    :param service_path: 服务包路径
    :param version: 版本信息
    :return: True/False，[信息列表]
    """

    # 升级前后版本检查
    if not pre_check(service_path=service_path, version=version):
        return False
    # 各服务自定义处理接口
    if not revoke_custom_upgrade_method("custom_pre_upgrade", service_path, version):
        return False
    # 获取各个子版本并升序排序
    upgrade_version_list = get_pending_version_list(service_path, version, "upgrade")
    LOGGER.info("Find version list to upgrade: %s", str(upgrade_version_list))

    for sub_version in upgrade_version_list:
        # 执行各个子版本sub_upgrade.pre_dbupgrade
        result = revoke_custom_sub_method("upgrade", sub_version, "pre_dbupgrade", service_path, version)
        if result:
            LOGGER.info("Succeed to execute pre_dbupgrade of %s", sub_version)
        else:
            LOGGER.error("Failed to execute pre_dbupgrade of %s", sub_version)
            return False
    return True


@gkit_interface
def post_upgrade(service_path, version=None):
    """
    GKIT升级接口：升级处理
    根据升级前及升级后版本执行对应版本目录的数据库及定制处理脚本

    :param service_path: 服务包路径
    :param node_env: 节点信息
    :param paas_env: paas信息
    :param stack: 堆栈信息
    :param version: 版本信息
    :return: True/False，[信息列表]
    """
    # 获取各个子版本并升序排序
    upgrade_version_list = get_pending_version_list(service_path, version, "upgrade")
    upgrade_path = os.path.join(service_path, 'upgrade')
    LOGGER.info("Find version list to upgrade: %s", str(upgrade_version_list))

    for sub_version in upgrade_version_list:
        sub_path = os.path.join(upgrade_path, sub_version)
        # 执行高斯库SQL
        if os.path.exists(sub_path) and 'gssql' in os.listdir(sub_path):
            sql_path = os.path.join(sub_path, 'gssql')
            if not execute_sql(sql_path):
                return False

        # 执行各个子版本sub_upgrade.post_dbupgrade
        result = revoke_custom_sub_method("upgrade", sub_version, "post_dbupgrade", service_path, version)
        if result:
            LOGGER.info("Succeed to execute post_dbupgrade of %s", sub_version)
        else:
            LOGGER.error("Failed to execute post_dbupgrade of %s", sub_version)
            return False
    return upgrade_version(version)


def version_check(version: dict):
    """
    检查版本号是否合法

    :param version: 版本信息
    :return: True/False
    """
    LOGGER.info("origin_version: %s", version.get("origin_version"))
    LOGGER.info("target_version: %s", version.get("target_version"))
    if not is_legal_ver(version.get("origin_version")):
        LOGGER.error("origin_version is illegal")
        return False
    if not is_legal_ver(version.get("target_version")):
        LOGGER.error("target_version is illegal")
        return False
    if compare_versions(version.get("origin_version"), version.get("target_version")) != -1:
        LOGGER.error("origin_version is greater than target_version")
        return False
    return True


def is_int_number(test_str: str) -> bool:
    """
    检查字符串是否为整数

    :param test_str: 待检查字符串
    :return: True/False
    """
    try:
        int(test_str)
        return True
    except ValueError:
        return False


def is_legal_ver(test_ver: str) -> bool:
    """
    检查版本号是否合法

    :param test_ver: 版本号
    :return: True/False
    """
    # 合法ver定义为形似1.0.0的纯数字点分式
    for one_short in test_ver.split('.'):
        if not is_int_number(one_short):
            return False
    return True


def is_exist_gs_sql(service_path: str, version: dict):
    """
    检查当前升级路径是否存在gssql.zip

    :param service_path: 服务路径
    :param version: 版本信息
    :return: True/False
    """
    upgrade_list = get_pending_version_list(service_path, version, "upgrade")
    upgrade_path = os.path.join(service_path, 'upgrade')
    for one_sub_version in upgrade_list:
        if not os.path.exists(os.path.join(upgrade_path, one_sub_version)):
            return False
        if 'gssql' in os.listdir(os.path.join(upgrade_path, one_sub_version)):
            return True
    return False


def upgrade_version(version=None):
    target_version = version.get("target_version")
    configmap_name = 'its-version'
    if not check_configmap_exist(configmap_name):
        LOGGER.info("Config map %s is not exist", configmap_name)
        return False
    config_data = get_configmap(
        configmap_name=configmap_name, namespace=os.getenv("PROJECT_NAME")).get('data')
    if not config_data:
        LOGGER.error(f"Config map {configmap_name} config_data is empty")
        config_data = {}
    upgrade_chain = config_data.get("UPGRADE_CHAIN")
    if not upgrade_chain:
        LOGGER.error(f"Config map {configmap_name} UPGRADE_CHAIN is None")
        config_data["UPGRADE_CHAIN"] = json.dumps([target_version])
    upgrade_chains = json.loads(upgrade_chain)
    if target_version in upgrade_chains:
        return True
    upgrade_chains.append(target_version)
    config_data["UPGRADE_CHAIN"] = json.dumps(upgrade_chains)
    return update_configmap(configmap_name, config_data)


if __name__ == "__main__":
    cnai_upgrade_mode = os.environ.get('CNAI_UPGRADE_MODE', '')
    LOGGER.info(f'cnai_upgrade_mode is: {cnai_upgrade_mode}')
    if cnai_upgrade_mode and cnai_upgrade_mode != 'normal,upgrade':
        LOGGER.info('It is not normal upgrade scene. do nothing.')
        sys.exit(0)
    service_path, version_info, lifecycle = get_upgrade_params()
    # 执行升级任务
    if lifecycle == "post_upgrade":
        if post_upgrade(service_path, version_info):
            sys.exit(0)
        else:
            sys.exit(1)
    elif lifecycle == "pre_upgrade":
        if pre_upgrade(service_path, version_info):
            sys.exit(0)
        else:
            sys.exit(1)
