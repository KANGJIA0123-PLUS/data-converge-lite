#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import os
import time

from dsf.open_apis.install_api import OpenInstallApi

from .logger import LOGGER


def get_configmap_template(config_map_name) -> dict:
    """
    返回configmap模板

    :param config_map_name: configmap名称
    :return: configmap的数据
    """
    return {
        'kind': 'ConfigMap',
        'apiVersion': 'v1',
        'metadata': {
            'name': config_map_name,
            'namespace': os.getenv("PROJECT_NAME")
        },
        'data': {
        }
    }


def build_configmap(configmap_template, config_data):
    """
    更新configmap

    :param configmap_template: configmap模板
    :param config_data: config配置项
    :return:
    """
    configmap_template["data"] = config_data
    return configmap_template


def check_configmap(config_map_name, new_config_item) -> bool:
    """
    检查创建结果

    :param new_config_item: 新增配置卷项目内容
    :param config_map_name: 配置卷名称
    :return: 创建是否成功,True(成功),False(失败)
    """
    timeout = 10
    namespace = os.getenv("PROJECT_NAME")
    open_api = OpenInstallApi({})
    for i in range(0, timeout):
        flag, current_data = open_api.get_configmap_by_namespace(config_map_name, namespace)
        if not flag:
            LOGGER.error(f"Failed to get configmap %s", config_map_name)
            time.sleep(1)
            continue
        if list(new_config_item.items())[0] in current_data.get("data", {}).items():
            LOGGER.info(f"Check configmap %s create success:%s", config_map_name, i)
            return True
        else:
            LOGGER.error(f"Check configmap %s create fail:%s", config_map_name, i)
            time.sleep(1)
    return False


def check_configmap_exist(config_map_name) -> bool:
    """
    检查创建结果

    :param config_map_name: 配置卷名称
    :return: 创建是否成功,True(成功),False(失败)
    """
    timeout = 10
    namespace = os.getenv("PROJECT_NAME")
    open_api = OpenInstallApi({})
    for i in range(0, timeout):
        flag, current_data = open_api.get_configmap_by_namespace(config_map_name, namespace)
        if not flag:
            LOGGER.error(f"Failed to get configmap %s", config_map_name)
            time.sleep(1)
            continue
        else:
            return True
    return False


def create_configmap(configmap_name: str, new_config_item: dict) -> bool:
    """
    创建配置卷

    :param configmap_name: configmap名称
    :param new_config_item: 新增的配置项目信息
    :return:
    """
    timeout = 10
    namespace = os.getenv("PROJECT_NAME")
    open_api = OpenInstallApi({})
    for _ in range(timeout):
        # 查询配置卷是否存在
        flag, configmap_old = open_api.get_configmap_by_namespace(configmap_name, namespace)
        LOGGER.info(f"Old configmap info %s", configmap_old)
        if flag and configmap_old:
            new_config_item = {**configmap_old.get("data", {}), **new_config_item}
            configmap_template = get_configmap_template(configmap_name)
            configmap_new = build_configmap(configmap_template, new_config_item)
            if not open_api.delete_configmap(configmap_name):
                LOGGER.error(f"Failed to delete configmap %s", configmap_name)
        else:
            configmap_template = get_configmap_template(configmap_name)
            configmap_new = build_configmap(configmap_template, new_config_item)

        LOGGER.info(f"New configmap info %s", configmap_new)
        flag, rsp = open_api.create_configmap(configmap_new)
        print(f"OOOOOO: {flag}       {rsp}")
        if not flag:
            LOGGER.error(f"Create configmap of %s failed", configmap_name)
        if check_configmap(configmap_name, new_config_item):
            return True
        time.sleep(5)
    return check_configmap(configmap_name, new_config_item)


def get_configmap(configmap_name: str, namespace=None) -> dict:
    """
    获取配置卷信息

    :param namespace: 命名空间
    :param configmap_name: 配置卷名称
    :return: configmap配置项
    """
    open_api = OpenInstallApi({})
    if namespace is None:
        namespace = os.getenv("PROJECT_NAME")
    flag, configmap = open_api.get_configmap_by_namespace(configmap_name, namespace)
    if not flag:
        LOGGER.error(f"Failed to get configmap %s", configmap_name)
        return {}
    return configmap


def get_configmap_value(configmap_name: str, key_name: str, namespace=None) -> dict:
    """
    获取配置卷信息

    :param namespace: 命名空间
    :param key_name: key名称
    :param configmap_name: 配置卷名称
    :return: configmap配置项
    """
    return get_configmap(configmap_name, namespace).get("data", {}).get(key_name)


def update_configmap(configmap_name: str, new_config_item: dict) -> bool:
    """
    更新配置卷内容

    :param configmap_name: 配置卷名称
    :param new_config_item: 新配置项
    :return: True:更新成功/False:更新失败
    """
    return create_configmap(configmap_name, new_config_item)


def delete_configmap(configmap_name):
    """
    删除Configmap

    :param configmap_name:
    :return: True:删除成功/False:删除失败
    """
    open_api = OpenInstallApi({})
    return open_api.delete_configmap(configmap_name)
