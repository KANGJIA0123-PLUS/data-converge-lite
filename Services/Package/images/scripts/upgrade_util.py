#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import functools
import json
import math
import os
import sys
from functools import wraps
from importlib.util import module_from_spec
from importlib.util import spec_from_file_location
from random import randint

from its_api import build_db_info_from_secret_new
from its_api import exec_gauss_sql
from its_api.configmap import get_configmap, create_configmap
from its_api.logger import LOGGER

# 导入upgrade_constant模块
CUSTOM_UPGRADE_LIST = ['upgrade_custom.py']
CUSTOM_SUB_UPGRADE_LIST = ['sub_upgrade_custom.py']
CUSTOM_SUB_ROLLBACK_LIST = ['sub_rollback_custom.py']

# 动态导入本地文件
current_path = os.path.split(__file__)[0]
upgrade_constant_path = os.path.join(current_path, 'upgrade_constant.py')
if os.path.isfile(upgrade_constant_path):
    upgrade_constant_spec = spec_from_file_location(f'upgrade_constant_{randint(0, 999999)}', upgrade_constant_path)
    upgrade_constant = module_from_spec(upgrade_constant_spec)
    upgrade_constant_spec.loader.exec_module(upgrade_constant)
    # 导入高斯配置
    GAUSS_SECRET_NAME = upgrade_constant.GAUSS_SECRET_NAME
    GAUSS_SECRET_CONFIG_NAME = upgrade_constant.GAUSS_SECRET_CONFIG_NAME
    GAUSS_USER_NAME = upgrade_constant.GAUSS_USER_NAME
    GAUSS_SCHEMA_NAME = upgrade_constant.GAUSS_SCHEMA_NAME
else:
    GAUSS_SECRET_NAME = ""
    GAUSS_SECRET_CONFIG_NAME = ""
    GAUSS_USER_NAME = ""
    GAUSS_SCHEMA_NAME = ""

log_path = os.getenv("LOG_PATH")
LOGGER.set_logger(os.path.join(log_path, "upgrade.log"))


def gkit_interface_params(func):
    """
    针对gkit接口的装饰器，记录版本相关日志

    :param func: 装饰函数
    :return: 装饰后函数
    """

    @wraps(func)
    def wrapper(service_path, target_input_json=None):
        LOGGER.info("Begin to execute gkit interface: %s, target_input_json: %s", func.__name__,
                    target_input_json)
        return func(service_path,
                    target_input_json=target_input_json)

    return wrapper


def gkit_interface(func):
    """
    针对gkit接口的装饰器，记录版本相关日志

    :param func: 装饰函数
    :return: 装饰后函数
    """

    @wraps(func)
    def wrapper(service_path, version=None):
        LOGGER.info("Begin to execute gkit interface: %s,  origin_version: %s, target_version: "
                    "%s", func.__name__, version.get("origin_version"),
                    version.get("target_version"))
        return func(service_path, version=version)

    return wrapper


def import_custom_upgrade(scripts_path, custom_upgrade_file):
    """
    导入自定义升级文件

    :param scripts_path: 服务scripts路径
    :param custom_upgrade_file: 自定义升级文件
    :return:
    """
    custom_upgrade_path = os.path.join(scripts_path, custom_upgrade_file)
    custom_uniq_module_name = f"{custom_upgrade_file[:-3]}_{randint(0, 999999)}"
    LOGGER.info("Find custom script %s", custom_upgrade_path)

    # 导入自定义升级文件并调用对应方法
    LOGGER.debug(f"Load uniq module %s from %s", custom_uniq_module_name, custom_upgrade_path)
    custom_upgrade_spec = spec_from_file_location(custom_uniq_module_name, custom_upgrade_path)
    custom_upgrade = module_from_spec(custom_upgrade_spec)
    custom_upgrade_spec.loader.exec_module(custom_upgrade)
    return custom_upgrade


def revoke_custom_params_method(method_name, service_path,
                                target_input_json=None):
    """
    调用自定义升级/回退方法

    :param target_input_json: 堆栈升级input_json字典
    :param method_name: 方法名称
    :param service_path: 服务包路径
    :return: True/False，[信息列表]
    """
    # 获取当前服务的自定义升级文件
    scripts_path = os.path.join(service_path, "scripts")
    current_files = os.listdir(scripts_path)
    custom_upgrade_files = [i for i in current_files if i in CUSTOM_UPGRADE_LIST]
    if len(custom_upgrade_files) != 1:
        LOGGER.warning("Can not find specific custom upgrade file in current service %s, please check. "
                       "custom_upgrade_files: %s", service_path, custom_upgrade_files)
        return True

    custom_upgrade_file = custom_upgrade_files[0]
    custom_upgrade = import_custom_upgrade(scripts_path, custom_upgrade_file)

    if method_name not in dir(custom_upgrade):
        LOGGER.warning("Can not find method %s in custom upgrade file %s", method_name, custom_upgrade_file)
        return True

    custom_method = getattr(custom_upgrade, method_name)
    result = custom_method(service_path, target_input_json)
    if result:
        LOGGER.info("Succeed to execute %s of %s, return info: %s", method_name, custom_upgrade_file)
        return True
    else:
        LOGGER.error("Failed to execute %s of %s, return info: %s", method_name, custom_upgrade_file)
        return False


def revoke_custom_upgrade_method(method_name, service_path, version=None):
    """
    调用自定义升级/回退方法

    :param method_name: 方法名称
    :param service_path: 服务包路径
    :param version: 版本信息
    :return: True/False，[信息列表]
    """
    # 获取当前服务的自定义升级文件
    scripts_path = os.path.join(service_path, "scripts")
    current_files = os.listdir(scripts_path)
    custom_upgrade_files = [i for i in current_files if i in CUSTOM_UPGRADE_LIST]
    if len(custom_upgrade_files) != 1:
        LOGGER.warning("Can not find specific custom upgrade file in current service %s, please check. "
                       "custom_upgrade_files: %s", service_path, custom_upgrade_files)
        return True

    custom_upgrade_file = custom_upgrade_files[0]
    custom_upgrade = import_custom_upgrade(scripts_path, custom_upgrade_file)

    if method_name not in dir(custom_upgrade):
        LOGGER.warning("Can not find method %s in custom upgrade file %s", method_name, custom_upgrade_file)
        return True

    custom_method = getattr(custom_upgrade, method_name)
    result = custom_method(service_path, version)
    if result:
        LOGGER.info("Succeed to execute %s of %s, return info: %s", method_name, custom_upgrade_file)
        return True
    else:
        LOGGER.error("Failed to execute %s of %s, return info: %s", method_name, custom_upgrade_file)
        return False


def revoke_custom_sub_method(operate, sub_version, method_name, service_path, version=None):
    """
    调用子版本升级/回退中自定义的sub方法

    :param operate: 调用操作[upgrade,rollback]
    :param sub_version: 待执行的子版本
    :param method_name: 方法名称
    :param service_path: 服务包路径
    :param version: 版本信息
    :return: True/False，[信息列表]
    """
    # 校验入参
    if operate not in ["upgrade", "rollback"]:
        LOGGER.error("Error get operate: %s in revoke_custom_sub_method", operate)
        return False

    # 准备变量
    if operate == "upgrade":
        custom_sub_list = CUSTOM_SUB_UPGRADE_LIST
    else:
        custom_sub_list = CUSTOM_SUB_ROLLBACK_LIST

    operate_path = os.path.join(service_path, operate)

    sub_operate_path = os.path.join(operate_path, sub_version)
    if not os.path.exists(sub_operate_path):
        LOGGER.warning("Can not find sub operate path of %s, revoke abort", sub_operate_path)
        return True

    sub_files = os.listdir(sub_operate_path)
    custom_sub_operate_files = [i for i in sub_files if i in custom_sub_list]
    if len(custom_sub_operate_files) == 0:
        LOGGER.warning("Can not find custom sub operate script in %s, revoke abort", sub_operate_path)
        return True

    if len(custom_sub_operate_files) > 1:
        LOGGER.info("Find error custom sub operate script in %s, custom_sub_operate_files:%s", sub_operate_path,
                    custom_sub_operate_files)
        return False

    custom_sub_operate_file = custom_sub_operate_files[0]
    custom_sub_operate_path = os.path.join(sub_operate_path, custom_sub_operate_file)
    custom_uniq_sub_operate_module_name = f"{custom_sub_operate_file[:-3]}_{randint(0, 999999)}"
    LOGGER.info("Find custom sub script %s", custom_sub_operate_path)

    # 导入自定义升级文件并调用对应方法
    LOGGER.debug(f"Load uniq sub module %s from %s", custom_uniq_sub_operate_module_name, custom_sub_operate_path)
    custom_sub_operate_spec = spec_from_file_location(custom_uniq_sub_operate_module_name, custom_sub_operate_path)
    custom_sub_operate = module_from_spec(custom_sub_operate_spec)
    custom_sub_operate_spec.loader.exec_module(custom_sub_operate)

    if method_name not in dir(custom_sub_operate):
        LOGGER.error("Can not find method %s in custom sub operate file %s", method_name, custom_sub_operate_file)
        return False

    custom_sub_method = getattr(custom_sub_operate, method_name)
    result = custom_sub_method(service_path, version)
    if result:
        LOGGER.info("Succeed to execute %s of %s", method_name, custom_sub_operate_file)
        return True
    else:
        LOGGER.error("Failed to execute %s of %s", method_name, custom_sub_operate_file)
        return False


def compare_versions(ver_a: str, ver_b: str):
    """
    比较两个版本号

    :param ver_a: 版本号a
    :param ver_b: 版本号b
    :return:
    """
    # ver参数格式： x.y.z
    # ver_a > ver_b时: return 1
    # ver_a = ver_b时: return 0
    # ver_a < ver_b时: return -1
    ver_atone_a, ver_atone_b = 0, 0
    ver_len = max(len(ver_a.split('.')), len(ver_b.split('.')))
    for _ in range(ver_len):
        ver_max = max(1, int(ver_a.split('.')[_]), int(ver_b.split('.')[_]))
        ver_atone_a *= pow(10, int(math.log10(ver_max)) + 1)
        ver_atone_b *= pow(10, int(math.log10(ver_max)) + 1)
        ver_atone_a += int(ver_a.split('.')[_])
        ver_atone_b += int(ver_b.split('.')[_])
    if ver_atone_a == ver_atone_b:
        return 0
    elif ver_atone_a > ver_atone_b:
        return 1
    elif ver_atone_a < ver_atone_b:
        return -1
    else:
        return 255


def get_pending_version_list(service_path: str, version_info: dict, operate: str) -> list:
    """
    获取待升级/回退版本列表

    :param service_path: 服务路径
    :param operate: 升级/回退操作["upgrade","rollback"]
    :param version_info: 版本信息
    :return: 升级版本列表
    """
    if operate not in ["upgrade", "rollback"]:
        LOGGER.error("Error occurred during get operate:%s", operate)
        raise ValueError

    pending_version_list = []
    if operate == "upgrade":
        reverse = False
    else:
        reverse = True
    operate_path = os.path.join(service_path, operate)
    if not os.path.exists(operate_path):
        return pending_version_list

    version_list = os.listdir(operate_path)
    version_list.sort(key=functools.cmp_to_key(compare_versions), reverse=reverse)

    origin_version = version_info.get("origin_version")
    target_version = version_info.get("target_version")
    max_version = max(origin_version, target_version, key=functools.cmp_to_key(compare_versions))
    min_version = min(origin_version, target_version, key=functools.cmp_to_key(compare_versions))
    # 遍历upgrade_path中的各个子版本
    for version in version_list:
        # 大于min_version，小于等于max_version则加入upgrade_list
        if compare_versions(version, min_version) == 1 and compare_versions(version, max_version) in [0, -1]:
            pending_version_list.append(version)
    return pending_version_list


def execute_sql(sql_path):
    """
    执行高斯SQL

    :param paas_env: paas环境变量: {"db": {"secret_key": 高斯卷KEY, "secret_name": 高斯卷名称, "db_user": 高斯用户, "db_name": 高斯SCH名}}
    :param sql_path: sql路径
    :return:
    """

    db_info = build_db_info_from_secret_new(GAUSS_SECRET_NAME,
                                            GAUSS_SECRET_CONFIG_NAME,
                                            GAUSS_USER_NAME,
                                            GAUSS_SCHEMA_NAME)
    return exec_gauss_sql(db_info, sql_path)


def sort_versions_exclude_current(version_list, current_version):
    """
    排除当前版本并从小到大排序

    Args:
        version_list: 版本号列表
        current_version: 当前版本号

    Returns:
        排除当前版本后的排序列表
    """
    if not version_list:
        return []
    return sorted(
        [v for v in version_list if v != current_version],
        key=lambda x: [int(i) for i in x.split('.')]
    )


def _validate_args(args: list, service_name: str, target_version: str):
    """
    校验命令行参数和服务名

    :param args: 命令行参数
    :param service_name: 服务名
    :param target_version: 目标版本
    """
    if len(args) == 0 or service_name is None:
        LOGGER.error("Env SERVICE_NAME is None or args is Empty")
        sys.exit(1)
    if target_version is None:
        LOGGER.error("Env VERSION is None")
        sys.exit(1)


def _get_upgrade_chains() -> list:
    """
    从ConfigMap获取升级版本链路

    :return: 排序后的版本列表
    """
    configmap_name = 'its-version'
    config_data = get_configmap(
        configmap_name=configmap_name, namespace=os.getenv("PROJECT_NAME")).get('data')
    if not config_data:
        LOGGER.error("configmap its-version is None or args is Empty")
        sys.exit(1)
    upgrade_chain = config_data.get("UPGRADE_CHAIN")
    if not upgrade_chain:
        LOGGER.error("Config map UPGRADE_CHAIN is None")
        sys.exit(1)
    return sorted(json.loads(upgrade_chain), key=functools.cmp_to_key(compare_versions))


def _check_and_get_version_info(args: list, upgrade_chains: list, target_version: str) -> dict:
    """
    检查是否需要升级/回滚，并返回版本信息

    :param args: 命令行参数
    :param upgrade_chains: 升级链路
    :param target_version: 目标版本
    :return: 版本信息字典或None
    """
    action = args[0]

    if action.__contains__("upgrade"):
        return _handle_upgrade(action, upgrade_chains, target_version)
    if action.__contains__("rollback"):
        return _handle_rollback(action, upgrade_chains, target_version)

    return None


def _handle_upgrade(action: str, upgrade_chains: list, target_version: str) -> dict:
    """
    处理升级场景

    :param action: 操作类型
    :param upgrade_chains: 升级链路
    :param target_version: 目标版本
    :return: 版本信息字典
    """
    if not upgrade_chains:
        LOGGER.info(f"No need to {action}")
        sys.exit(0)

    if upgrade_chains[-1] == target_version:
        if len(upgrade_chains) >= 2:
            origin_version = upgrade_chains[-2]
            return {
                "origin_version": origin_version,
                "target_version": target_version
            }
        else:
            LOGGER.info("First deployment, no previous version exists")
            sys.exit(0)

    origin_version = upgrade_chains[-1]
    return {
        "origin_version": origin_version,
        "target_version": target_version
    }


def _handle_rollback(action: str, upgrade_chains: list, target_version: str) -> dict:
    """
    处理回滚场景

    :param action: 操作类型
    :param upgrade_chains: 升级链路
    :param target_version: 目标版本
    :return: 版本信息字典
    """
    if not upgrade_chains:
        LOGGER.info(f"No need to {action}")
        sys.exit(0)

    if upgrade_chains[-1] != target_version:
        origin_version = upgrade_chains[-1]
        return {
            "origin_version": origin_version,
            "target_version": target_version
        }

    origin_version = upgrade_chains[-2] if len(upgrade_chains) > 1 else upgrade_chains[-1]
    return {
        "origin_version": origin_version,
        "target_version": target_version
    }


def get_upgrade_params() -> tuple[str, dict, str]:
    """
    获取升级参数

    :return: (service_path, version_info, action)
    """
    args = sys.argv[1:]
    service_name = os.getenv("SERVICE_NAME")
    target_version = os.getenv("VERSION")

    _validate_args(args, service_name, target_version)

    LOGGER.set_logger(os.path.join(log_path, f"{args[0]}.log"))
    service_path = os.path.join("/opt/its", service_name)

    upgrade_chains = _get_upgrade_chains()
    version_info = _check_and_get_version_info(args, upgrade_chains, target_version)

    if version_info is None:
        LOGGER.error("Unknown action: %s", args[0])
        sys.exit(1)

    return service_path, version_info, args[0]
