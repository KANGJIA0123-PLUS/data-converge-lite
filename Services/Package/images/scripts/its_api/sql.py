#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import os
import re
import time
from pathlib import Path

from dsf.crypto.xms_crypto import xms_encrypt as xms_default_encrypt
from dsf.open_apis.install_api import OpenInstallApi

from .logger import LOGGER
from .secret import get_secret
from .secret import load_pwd_config

# 初始化OpenInstallApi
init_env = {}
open_api = OpenInstallApi(init_env)


def compare_versions(ver_a: str, ver_b: str):
    """
    比较两个版本号

    :param ver_a: 版本号a
    :param ver_b: 版本号b
    :return:
    """
    ver_a = ver_a.split("/")[-2]
    ver_b = ver_b.split("/")[-2]
    # ver参数格式： x.y.z
    # ver_a > ver_b时: return 1
    # ver_a = ver_b时: return 0
    # ver_a < ver_b时: return -1
    ver_a_list = ver_a.split(".")
    ver_b_list = ver_b.split(".")
    ver_a_list.extend("0" * (3 - len(ver_a_list)))
    ver_b_list.extend("0" * (3 - len(ver_b_list)))
    for i in range(3):
        if int(ver_a_list[i]) > int(ver_b_list[i]):
            return 1
        elif int(ver_a_list[i]) < int(ver_b_list[i]):
            return -1
    return 0


def get_service_name(sql_path):
    """
    获取服务名称

    :param sql_path: sql路径
    :return: 服务名称
    """
    temp_path_list = sql_path.split("/")
    temp_path_list.reverse()
    return temp_path_list[temp_path_list.index("tenant") - 1]


def get_config_file(sql_path):
    """
    获取配置文件路径

    :param sql_path: sql路径
    :return: 配置文件路径
    """
    service_name = get_service_name(sql_path)
    service_name_without_version = service_name.split('-')[:-1]
    config_file_name = "-".join(service_name_without_version) + ".ini"
    os.path.join("/opt/slm/config_ext", config_file_name)
    return os.path.join("/opt/slm/config_ext", config_file_name)


def check_sql_record(sql_path):
    """
    检查sql执行记录

    :param sql_path: sql执行路径
    :return: True:sql执行过，False:sql没执行过
    """
    config_file = get_config_file(sql_path)
    service_name = get_service_name(sql_path)
    sql_path_name = sql_path.split(service_name)[-1].strip("/")

    if not os.path.isfile(config_file):
        LOGGER.info("Can not find sql record file")
        return False

    with open(config_file, "r", encoding="utf-8") as f:
        for line in f.readlines():
            if line.startswith(f"{sql_path_name}="):
                return True
    return False


def record_sql_execution(sql_path):
    """
    写入sql执行揭露

    :param sql_path: sql路径
    :return:
    """
    config_file = get_config_file(sql_path)
    service_name = get_service_name(sql_path)
    sql_path_name = sql_path.split(service_name)[-1].strip("/")
    execute_date = time.strftime("%Y-%m-%d %H:%M:%S %z", time.localtime())

    fd = os.open(config_file, os.O_WRONLY | os.O_CREAT, int("0600", 8))
    with os.fdopen(fd, "a", encoding="utf-8") as f:
        f.write(f"{sql_path_name}={execute_date}\n")


def list_sql_files(path):
    """
    递归列出所有的sql文件

    :param path: 递归的入口路径
    :return: 所有SQL文件的generator
    """
    for root, _, files in os.walk(path):
        for name in [i for i in files if i.lower().endswith(".sql")]:
            yield os.path.join(root, name)


def get_sql_path_list(sql_path: str, recurse_path: bool) -> list:
    """
    获取路径列表

    :param sql_path: sql路径
    :param recurse_path: 是否递归路径
    :return: sql
    """
    if not recurse_path:
        if not os.path.exists(sql_path):
            LOGGER.info("Can not find sql_path of %s, please check", sql_path)
            return []
        return [sql_path]

    path_list = []
    for i in list_sql_files(sql_path):
        path_list.append(os.path.dirname(i))
    return list(set(path_list))


def check_db_info(db_info: dict):
    """
    校验db_info参数

    :param db_info: db_info字典参数
    :return: 是否校验成功，布尔值
    """
    db_ip = db_info.get('db_ip')
    db_port = db_info.get('db_port')
    schema_name = db_info.get('schema_name')
    user_name = db_info.get('user_name')
    user_encrypt_pwd = db_info.get('user_encrypt_pwd')

    if not db_ip or not db_port or not schema_name:
        LOGGER.error("Can not find db_ip or db_port or schema_name from db_info, please check")
        return False
    if not user_name or not user_encrypt_pwd:
        LOGGER.error("Can not find user_name or user_encrypt_pwd from db_info, please check")
        return False
    return True


def exec_gauss_sql(db_info, sql_path: str, delimiter="@@"):
    """
    初始化数据库

    :param node_env: 节点信息
    :param recurse_path: 是否遍历sql路径，默认为False
    :param record_exec: 是否记录sql执行
    :param sql_path: sql路径，sql命名格式：数字编号-SQL名称.sql，
                     sql默认定界符为分号（;），如果需要自定义定界符，可以在第一行添加注解，格式如下（以@@作为定界符）：--delimiter=@@
    :param db_info: 数据库信息，格式形如：{ "db_ip": ip,
                                        "db_port": port,
                                        "schema_name": schema_name,
                                        "user_name": user_name,
                                        "user_encrypt_pwd": user_encrypt_pwd }
    :return: True执行成功，False执行失败
    """
    LOGGER.info("Start to execute Gauss SQL")

    db_ip = db_info.get('db_ip')
    db_port = db_info.get('db_port')
    user_name = db_info.get('user_name')
    user_encrypt_pwd = db_info.get('user_encrypt_pwd')

    # 如果 sql_path 是目录，查找其中所有的 .sql 文件
    if os.path.isdir(sql_path):
        sql_files = sorted(Path(sql_path).rglob('*.sql'))
        if not sql_files:
            return False

        # 依次执行每个 SQL 文件，任一失败则返回 False
        for sql_file in sql_files:
            LOGGER.info("Execute sql file name of %s", sql_file)
            if not open_api.batch_execute(str(sql_file), delimiter, db_ip, db_port, user_name, user_encrypt_pwd):
                return False
        return True
    else:
        # sql_path 是单个文件，直接执行
        LOGGER.info("Execute sql file name of %s", sql_path)
        return open_api.batch_execute(sql_path, delimiter, db_ip, db_port, user_name, user_encrypt_pwd)


def is_ip(ip_address):
    """
    检查是否为合法IP地址
    :param ip_address:
    :return:
    """
    rex = r"^(([01]?\d\d?|2[0-4]\d|25[0-5])\.){3}([01]?\d\d?|2[0-4]\d|25[0-5])$"
    p = re.compile(rex)
    if p.match(ip_address):
        return True
    else:
        LOGGER.error("Check IP %s failed, invalid IP address", ip_address)
        return False


def build_db_info_from_secret(secret_name, config_name, user_name, schema_name):
    """
    根据secret卷获取数据库信息

    :param schema_name: schema名称
    :param secret_name: secret卷名称
    :param config_name: 配置名称
    :param user_name: 用户名称
    :return: 用户密码、数据库IP、端口
    """
    secret_info = get_secret(secret_name)
    password_config = load_pwd_config(secret_info)
    paas_config = [i for i in password_config.get("paasconfig") if i.get("name") == config_name][0]
    for user_config in paas_config.get("info"):
        if user_config.get("user") == user_name:
            user_pwd = user_config.get("password")
            ip = paas_config.get("IPS").rsplit(":", 1)[0]
            port = paas_config.get("IPS").rsplit(":", 1)[1]
            return {
                "db_ip": ip,
                "db_port": port,
                "schema_name": schema_name,
                "user_name": user_name,
                "user_encrypt_pwd": xms_default_encrypt(user_pwd)
            }
    return {}


def build_db_info_from_secret_new(secret_name, secret_key, user_name, schema_name):
    """
    根据secret卷获取数据库信息

    :param schema_name: schema名称
    :param secret_name: secret卷名称
    :param secret_key: 密码名称
    :param user_name: 用户名称
    :return: 用户密码、数据库IP、端口
    """
    secret_info = get_secret(secret_name)
    password_config = load_pwd_config(secret_info, secret_key, True)
    for user_config in password_config.get("info"):
        if user_config.get("user") == user_name:
            user_pwd = user_config.get("password")
            ip = password_config.get("IPS").rsplit(":", 1)[0]
            port = password_config.get("IPS").rsplit(":", 1)[1]
            return {
                "db_ip": ip,
                "db_port": port,
                "schema_name": schema_name,
                "user_name": user_name,
                "user_encrypt_pwd": user_pwd
            }
    return {}
