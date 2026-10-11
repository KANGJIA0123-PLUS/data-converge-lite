#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import base64
import json
import os
import time

from dsf.crypto.xms_crypto import xms_decrypt as xms_default_decypt
from dsf.crypto.xms_crypto import xms_encrypt as xms_default_encrypt
from dsf.open_apis.install_api import OpenInstallApi

from .logger import LOGGER


def get_pwd_config_item(item_name, item_type, ip, region_type, user_pwd_tuple_list: list) -> json:
    """
    获取密码配置item

    :param user_pwd_tuple_list: 用户密码元组列表，如：[("user1","pwd1"),("user2","pwd2")]
    :param region_type: 区域类型
    :param ip: IP地址
    :param item_type: secret item类型
    :param item_name: secret item名称
    :return: secret item
    """
    return {
        "name": item_name,
        "type": item_type,
        "IPS": ip,
        "regionType": region_type,
        "info": [dict(zip(["user", "password"], i)) for i in user_pwd_tuple_list]
    }


def load_pwd_config(secret_info, secret_key='password', isBase64=False) -> dict:
    """
    加载当前密钥卷中的密码配置

    :param secret_info: 密钥卷信息
    :param secret_key: 密码信息
    :param isBase64: 密码是否只是base64加密
    :return: password json
    """
    password_data = secret_info['data'].get(secret_key)
    if isBase64:
        bytes_data = base64.b64decode(password_data).decode()
        current_pass_config = bytes_data.split("content: '")[1].split("'")[0]
    else:
        current_pass_config = base64.b64decode(
            xms_default_decypt(str(base64.b64decode(password_data), "utf-8")))
    return json.loads(current_pass_config)


def update_pwd_config_item(secret_info, password_item) -> list:
    """
    更新密码配置项目

    :param password_item: 密码描述
    :param secret_info: 密码卷信息
    :return: 更新后的密码配置项目
    """
    current_password_config = load_pwd_config(secret_info)
    password_config_name = password_item.get('name')
    password_config_item_list = [
        i
        for i in current_password_config['paasconfig']
        if i.get('name') != password_config_name
    ]
    password_config_item_list.append(password_item)
    return password_config_item_list


def create_pwd_secret(secret_name: str, password_item: dict):
    """
    创建密钥卷

    :param password_item: 密码描述
    :param secret_name: 密码卷名称，名称中以短横线分割，不能包括下划线
    :return: True:创建成功/False:创建失败
    """
    timeout = 10
    open_api = OpenInstallApi({})
    for i in range(timeout):
        LOGGER.info(f"Try to create secret %s for the %s time", secret_name, i)
        # 查询密钥卷是否存在
        flag, secret_info = open_api.get_secret(secret_name)
        if flag and secret_info:
            password_config_item = update_pwd_config_item(secret_info, password_item)
            if not open_api.delete_secret(secret_name):
                LOGGER.error(f"Failed to delete secret %s", secret_name)
        else:
            password_config_item = [password_item]

        # flag取值为true/false，crypt表示是对value进行default加密
        if not open_api.create_secret(secret_name, build_pwd(password_config_item), crypt=False):
            LOGGER.error(f"create secret failed:%s", secret_name)
        if check_create(secret_name, password_item):
            return True
        time.sleep(5)
    return check_create(secret_name, password_item)


def update_pwd_secret(secret_name: str, password_item: dict):
    """
    更新secret

    :param secret_name: secret名称
    :param password_item: 密码配置项
    :return: True:更新成功/False:更新失败
    """
    return create_pwd_secret(secret_name, password_item)


def get_secret(secret_name: str):
    """
    获取secret卷

    :param secret_name: secret名称
    :return:
    """
    open_api = OpenInstallApi({})
    flag, secret_info = open_api.get_secret(secret_name)
    if not flag:
        LOGGER.error(f"Failed to get secret %s", secret_name)
        return {}
    return secret_info


def delete_secret(secret_name: str):
    """
    删除secret卷

    :param secret_name: secret卷名称
    :return:
    """
    open_api = OpenInstallApi({})
    return open_api.delete_secret(secret_name)


def check_create(secret_name, password_item):
    """
    检查创建结果

    :param secret_name: 密码卷名称
    :param password_item: 密码描述
    :return: 创建是否成功,True(成功),False(失败)
    """
    timeout = 10
    open_api = OpenInstallApi({})
    for i in range(0, timeout):
        flag, secret_info = open_api.get_secret(secret_name)
        if not flag:
            LOGGER.error(f"Failed to get secret %s", secret_name)
            time.sleep(1)
            continue
        all_password = load_pwd_config(secret_info)
        if password_item in all_password["paasconfig"]:
            LOGGER.info(f"Check password item create success:%s", i)
            return True
        else:
            LOGGER.error(f"Check password item create fail:%s", i)
            time.sleep(1)
    return False


def build_pwd(password_config_item_list: list):
    """
    拼接密码json

    :param password_config_item_list: 密码配置列表
    :return: 密码信息
    """
    paasconfig_key = "paasconfig"
    password = {paasconfig_key: password_config_item_list}

    info_key = "password"
    password_info = {
        info_key: str(base64.b64encode(
            xms_default_encrypt(
                str(base64.b64encode(json.dumps(password).encode('utf-8')),
                    'utf-8')).encode('utf-8')), 'utf-8')
    }

    LOGGER.info(f"Build password info success.")
    return password_info


def create_product_ca(product_name):
    """
    创建产品二级CA证书

    :param product_name: 产品名称
    :return:
    """
    namespace = os.getenv("NAMESPACE")
    open_api = OpenInstallApi({})

    LOGGER.info(f"Creating product %s CA for namespace: %s", product_name, namespace)
    data = {
        'apiVersion': 'secret.com.huawei/v1',
        'kind': 'CertificateAuthority',
        'metadata': {
            'name': f'{product_name}-ca-crd',
            'namespace': namespace,
            'labels': {
                'app': product_name
            }
        },
        'spec': {
            'secretName': f'{product_name}-ca-secret',
            'subject': {
                'organizations': ['Huawei Technologies'],
                'countries': ['CN'],
                'organizationalUnits': ['GTS'],
                'localities': ['ShenZhen'],
                'provinces': ['Guangdong'],
                'commonName': product_name
            },
            'duration': '262800h',
            'renewBefore': '720h',
            'keyUsage': ['certificate signature', 'Certification Revocation List (CRL) signature'],
            'basicConstraints': ['Subject Type=CA'],
            'privateKey': {
                'rotationPolicy': 'Always',
                'encoding': 'PKCS8',
                'algorithm': 'RSA',
                'size': 3072
            }
        }
    }

    resp = open_api.add_certificate_authorities(data)
    if resp:
        LOGGER.info(f"Success to add %s CA crd", product_name)
        return True
    else:
        LOGGER.error(f"Failed to add %s CA crd", product_name)
        return False
