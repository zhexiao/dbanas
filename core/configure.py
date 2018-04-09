# -*- coding: utf-8 -*-
# !/usr/bin/python3
"""
入口文件
"""

import argparse
import configparser
from core.exceptions import ConfigError


class Configure(object):
    """
    命令行参数
    """

    def __init__(self):
        self.configure = None
        self.commands = None
        self.run()

    def run(self):
        self.parse_arg()

        # 验证文件
        config_file = self.commands.config
        if not config_file:
            raise ConfigError('缺少配置文件')

        self.read_config(config_file)
        self.validate_config()

    def parse_arg(self):
        parser = argparse.ArgumentParser(description='')
        parser.add_argument(
            '-c', '--config', help='配置文件'
        )
        parser.add_argument(
            '-f', '--folder', help='导出数据库文件到该目录'
        )

        self.commands = parser.parse_args()

    def read_config(self, config_file):
        self.configure = configparser.ConfigParser()
        self.configure.read(config_file)

    def validate_config(self):
        required_params = {
            'mysql': ['host', 'user', 'passwd', 'db'],
        }

        for _type, params in required_params.items():
            # 验证section
            if _type not in self.configure.sections():
                raise ConfigError('缺少配置模块：{0}'.format(_type))

            # 验证参数params
            for pm in params:
                if pm not in list(self.configure[_type]):
                    raise ConfigError('缺少配置变量：{0}下面需要{1}'.format(
                        _type, pm
                    ))

    def __getattr__(self, item):
        return self.configure[item]
