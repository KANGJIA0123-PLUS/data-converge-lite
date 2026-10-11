#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
import os
import traceback

from dsf.common.logger import EasierLogger


class CustomLogger:
    """
    自定义logger
    """
    _instance = None
    _log_dict = {}

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    @staticmethod
    def _get_caller_file():
        """
        根据当前的调用栈，获取调用方名称

        :return:
        """
        trace_list = traceback.format_stack()
        for i in sorted(trace_list, reverse=True):
            file_path = i.split('"')[1]
            file_name = os.path.basename(file_path)
            if file_name in ["pre-install.py", "post-install.py", "pre-delete.py", "post-delete.py", "upgrade.py", "rollback.py"]:
                return file_path
        return ""

    @staticmethod
    def _build_msg(msg: str, *params):
        """
        组装日志的message

        :param msg: 日志信息
        :param params: 日志参数
        :return:
        """
        for param in params:
            msg = msg.replace("%s", str(param), 1)
        return msg

    @staticmethod
    def _get_log_path(caller_file):
        """
        获取日志路径

        :param caller_file: 调用文件全路径
        :return: 日志路径
        """
        tenant_sub_dir = os.path.dirname(caller_file).split("tenant")[-1]
        return "-".join(os.path.dirname(tenant_sub_dir).split("/")[1].split("-")[:-1])

    def set_logger(self, log_path=""):
        """
        根据调用方创建slm的EasierLogger实例

        :param log_path: EasierLogger日志路径
        :return:
        """
        caller_file = self._get_caller_file()
        if not caller_file:
            return
        if log_path:
            tmp_logger = EasierLogger(log_path)
        else:
            tmp_logger = EasierLogger(self._get_log_path(caller_file))
        CustomLogger._log_dict[caller_file] = tmp_logger

    def debug(self, msg, *params):
        """
        调试日志信息

        :param msg: 日志信息
        :return:
        """
        self._get_logger().debug(self._build_msg(msg, *params))

    def error(self, msg, *params):
        """
        错误日志信息

        :param msg: 日志信息
        :return:
        """
        self._get_logger().error(self._build_msg(msg, *params))

    def info(self, msg, *params):
        """
        普通日志信息

        :param msg: 日志信息
        :return:
        """
        self._get_logger().info(self._build_msg(msg, *params))

    def warning(self, msg, *params):
        """
        警告日志信息

        :param msg: 日志信息
        :return:
        """
        self._get_logger().warning(self._build_msg(msg, *params))

    def critical(self, msg, *params):
        """
        致命日志信息

        :param msg: 日志信息
        :return:
        """
        self._get_logger().critical(self._build_msg(msg, *params))

    def _get_logger(self):
        """
        根据调用栈获取slm的EasierLogger实例

        :return:
        """
        caller_file = self._get_caller_file()
        logger = CustomLogger._log_dict.get(caller_file)
        if not logger:
            logger = EasierLogger("its")
        return logger


LOGGER = CustomLogger()
