#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import base64
import json
import os

import requests
from dsf.crypto.xms_crypto import xms_decrypt as xms_default_decypt
from dsf.open_apis.install_api import OpenInstallApi

from .logger import LOGGER


class GamMachineUserTool:
    def __init__(self, tenant_id):
        """
        GAM机机用户工具类

        :param tenant_id: 租户id
        """
        self.tenant_id = tenant_id
        self.token = None
        self.lb_ip = None
        self.lb_port = None
        self.open_api = OpenInstallApi({})

        namespace = os.getenv("NAMESPACE")
        lb_ipv4 = self._get_configmap_value("lb-cfg-configmap", "external_vip", namespace)
        lb_ipv6 = self._get_configmap_value("lb-cfg-configmap", "external_vip6", namespace)
        self.lb_ip = lb_ipv4 if lb_ipv4 and "default" != lb_ipv4 else f"[{lb_ipv6}]"
        self.lb_port = self._get_configmap_value(" ", "lb_external_port", namespace)

        self._get_token()

    @staticmethod
    def _resolve_response(response):
        """
        解析返回值，记录日志
        :param response: 请求返回实例
        :return: 请求是否成功，请求返回实例
        """
        result_code = "-1"
        try:
            message = json.loads(response.text)
            if "resultCode" in message:
                result_code = message.get("resultCode")
            if "resultMessage" in message:
                result_message = message.get("resultMessage")
                LOGGER.info(f"Result message: %s", result_message)
            if result_code == "0":
                return True
            return False
        except ValueError:
            return False

    def query_all_machine_roles(self):
        """
        查询所有机机用户角色

        :return: 所有角色信息
        """
        url = f"https://{self.lb_ip}:{self.lb_port}/gam/v1/tenants/{self.tenant_id}/machineroles"
        flag, response = self._request("GET", url)
        role_infos = []
        if flag:
            try:
                message = json.loads(response.text)
                if "result" in message:
                    result = message.get("result")
                    role_infos = result.get("roles", [])
            except ValueError:
                LOGGER.info("Query machine roles error!")

        LOGGER.info(f"Query machine roles: %s", role_infos)
        return role_infos

    def query_machine_user(self, machine_user_name):
        """
        查询机机用户

        :param machine_user_name: 机机用户名
        :return: 机机用户ID，激活状态
        """
        url = f"https://{self.lb_ip}:{self.lb_port}/gam/v1/tenants/{self.tenant_id}/machineuser?" \
              f"machineUserAccount={machine_user_name}"

        flag, response = self._request("GET", url)
        if not flag:
            return "", False
        try:
            message = json.loads(response.text)
            if "result" in message:
                result = message.get("result")
                user_infos = result.get("users")
                for user_info in user_infos:
                    machine_user_id = user_info.get("machineUserId", "")
                    is_activated = user_info.get("active", "1") == "1"
                    LOGGER.info(f"Query machine user %s id %s active %s", machine_user_name, machine_user_id,
                                is_activated)
                    return machine_user_id, is_activated
                return "", False
            else:
                return "", False
        except ValueError:
            return "", False

    def create_machine_user(self, machine_user_name, machine_user_desc):
        """
        创建机机用户

        :param machine_user_desc: 机机用户描述
        :param machine_user_name: 机机用户名
        :return: 机机用户信息
        """
        data = {
            "machineUserAccount": machine_user_name,
            "description": machine_user_desc,
            "expireDay": "2000",
            "isGlobal": "Y"
        }
        url = f"https://{self.lb_ip}:{self.lb_port}/gam/v1/tenants/{self.tenant_id}/machineuser"
        flag, rsp = self._request("POST", url, data)
        if not flag:
            LOGGER.error("Failed to request create machine user")
            return {}
        try:
            message = json.loads(rsp.text)
            if "result" in message:
                result = message.get("result")
                machine_user_id = result.get("machineUserId")
                machine_user_account = result.get("machineUserAccount")
                access_key = result.get("accessKey")
                # GAM接口响应中存在单词拼写错误
                secret_key = result.get("sercetKey")
                if not machine_user_id or not machine_user_account:
                    LOGGER.error(f"Create machine user %s failed, can not find user info", machine_user_name)
                    return {}
                if not access_key or not secret_key:
                    LOGGER.error(f"Create machine user %s failed, can not find ak/sk info", machine_user_name)
                    return {}

                LOGGER.info(f"Create machine user %s success", machine_user_name)
                return {
                    "machine_user_id": machine_user_id,
                    "machine_user_account": machine_user_account,
                    "access_key": access_key,
                    "secret_key": secret_key
                }
        except ValueError:
            LOGGER.error("ValueError occurred create machine user")
            return {}
        return {}

    def grant_machine_user_roles(self, machine_user_id, role_name_list: list):
        """
        给机机用户赋予角色

        :param role_name_list: 角色名称列表
        :param machine_user_id: 机机用户ID
        :return:
        """

        data = {
            "machineRoleIds": self._get_role_ids_from_name(role_name_list)
        }
        url = f"https://{self.lb_ip}:{self.lb_port}/gam/v1/tenants/{self.tenant_id}/machineusers/" \
              f"{machine_user_id}/grant-machine-roles"
        flag, response = self._request("POST", url, data)
        if flag:
            LOGGER.info(f"Succeed to grant machine roles %s to machine user %s", role_name_list, machine_user_id)
            return True
        else:
            LOGGER.error(f"Failed to grant machine roles %s to machine user %s", role_name_list, machine_user_id)
            return False

    def inactivate_machine_user(self, machine_user_id):
        """
        去激活机机用户

        :param machine_user_id: 机机用户ID
        :return:
        """
        data = {
            "machineUserIds": machine_user_id
        }
        url = f"https://{self.lb_ip}:{self.lb_port}/gam/v1/tenants/{self.tenant_id}/machineuser/inactive"
        flag, rsp = self._request("PATCH", url, data)
        if flag:
            LOGGER.info(f"Succeed to inactive user %s", machine_user_id)
            return True
        LOGGER.error(f"Failed to inactive user %s", machine_user_id)
        return False

    def delete_machine_user(self, machine_user_id):
        """
        删除机机用户

        :param machine_user_id: 机机用户ID
        :return:
        """
        url = f"https://{self.lb_ip}:{self.lb_port}/gam/v1/tenants/{self.tenant_id}/machineuser/{machine_user_id}"
        flag, rsp = self._request("DELETE", url)
        if flag:
            LOGGER.info(f"Succeed to delete user %s", machine_user_id)
            return True
        LOGGER.error(f"Failed to delete user %s", machine_user_id)
        return False

    def _get_secret_value(self, secret_name, item_key):
        """
        获取ak/sk卷中的值

        :param secret_name: 密码卷名称
        :param item_key: 密码卷中的key
        :return:
        """
        flag, secret_info = self.open_api.get_secret(secret_name)
        if not flag or not secret_info:
            LOGGER.error(f"Get secret %s failed", secret_name)
            return ""
        secret_data = secret_info.get("data", {}).get("gam-auth-default.properties")
        secret_items = str(base64.b64decode(secret_data), "utf-8").split()
        secret_dict = {i.split("=", 1)[0]: i.split("=", 1)[1] for i in secret_items}
        return secret_dict.get(item_key, "")

    def _get_configmap_value(self, config_map_name, item_key, namespace=None):
        """
        获取configmap的值

        :param config_map_name: configmap名称
        :param item_key: cm卷中的key
        :param namespace: 命名空间
        :return:
        """
        if namespace is None:
            namespace = os.getenv("NAMESPACE")
        flag, config_map_info = self.open_api.get_configmap_by_namespace(config_map_name, namespace)
        if not flag or not config_map_info:
            LOGGER.error(f"Get configmap %s failed", config_map_name)
            return ""
        return config_map_info.get("data", {}).get(item_key)

    def _get_token(self):
        """
        获取token的方法

        :return: token值
        """
        open_api = OpenInstallApi({})
        install_ak = self._get_secret_value("dsp-public-secret", "gam.install-access-key")
        install_sk = self._get_secret_value("dsp-public-secret", "gam.install-secret-key")
        decrypt_install_sk = str(xms_default_decypt(install_sk)).strip()
        response = open_api.get_token(install_ak, decrypt_install_sk, self.lb_ip, int(self.lb_port), self.tenant_id)
        if not response:
            LOGGER.error("Can not get token, response is null")
        if "X-Subject-Token" in response:
            token = response.get("X-Subject-Token")
            if not token:
                LOGGER.error("Can not get token, token is null")
            else:
                self.token = token
        else:
            LOGGER.error("Can not get token, can not find X-Subject-Token")

    def _request(self, method, url, data=None):
        """
        发起gam请求

        :param method: 请求方法
        :param url: 请求url
        :param data: 请求数据
        :return: 请求response
        """
        session = requests.Session()
        header = {"X-Access-Token": self.token, "Content-type": "application/json; charset=UTF-8"}
        LOGGER.info(f"Start request url: %s with data: %s", url, data)
        try:
            response = session.request(method, url, data=json.dumps(data), headers=header)
        except Exception as e:
            LOGGER.error(f"Error occurred during request: %s", e)
            return False, ""

        status_code = response.status_code
        LOGGER.info(f"The response status code is: %s", status_code)
        if status_code != 200:
            LOGGER.error(f"The response return error message is: %s", response.text)
            if method == "DELETE" and response.status_code == 500:
                # 标识删除的内容不存在，也是正常的
                LOGGER.info("Delete action: the resource is not exist")
                return True, ""
            else:
                LOGGER.info(f"Error occurred during request to %s", url)
                return False, response
        flag = self._resolve_response(response)
        return flag, response

    def _get_role_ids_from_name(self, role_name_list):
        """
        通过role_name获取role_id

        :param role_name_list:
        :return:
        """
        all_roles_info = self.query_all_machine_roles()
        role_id_list = []
        for role_name in role_name_list:
            for role_info in all_roles_info:
                if role_info["roleName"] == role_name:
                    role_id_list.append(role_info["roleId"])
        return role_id_list


def create_machine_user(tenant_id, machine_user_name, machine_user_desc, role_list):
    """
    创建机机用户入口

    :param tenant_id: tenant id（1000）
    :param machine_user_name: 机机用户名称
    :param machine_user_desc: 机机用户描述
    :param role_list: 角色名称列表
    :return: 生成的机机用户信息
    """
    gam_tool = GamMachineUserTool(tenant_id)
    # 检查是否有旧用户
    LOGGER.info("Check old machine user")
    old_machine_user_id, _ = gam_tool.query_machine_user(machine_user_name)
    if old_machine_user_id:
        # 清理旧用户
        LOGGER.info("Inactivate old machine user")
        gam_tool.inactivate_machine_user(old_machine_user_id)
        LOGGER.info("Delete old machine user")
        gam_tool.delete_machine_user(old_machine_user_id)
    # 创建新用户
    machine_user_info = gam_tool.create_machine_user(machine_user_name, machine_user_desc)
    if not machine_user_info:
        LOGGER.error("Create new machine user failed")
        return {}
    machine_user_id = machine_user_info.get("machine_user_id")
    LOGGER.info("Grant roles to new machine user")
    # 用户赋权
    if role_list:
        if not gam_tool.grant_machine_user_roles(machine_user_id, role_list):
            LOGGER.error("Error occurred during grant roles to gam machine user")
            return {}
    else:
        LOGGER.info("Can not find role list, grant machine user roles abort")
    return {
        "access_key": machine_user_info.get("access_key"),
        "secret_key": machine_user_info.get("secret_key"),
        "ip": gam_tool.lb_ip,
        "port": gam_tool.lb_port,
        "machine_user_name": machine_user_name
    }
