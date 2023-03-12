'''
*****************************************************************************
*File : wild_cm_conf.py
*Module : wild_cm 
*Purpose : CM configuration file validate and load into main program 
*Author : Sumankumar Panchal 
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

# '''import python packages'''
import json
import sys
from os import path
from typing import NamedTuple

import json_checker
from json_checker import Checker
from json_checker.core.exceptions import CheckerError

# '''Import WILD packages '''
from mvis_log import *


class WildCmConfRead:
    schema = {
        "COMMENT": str,
        "VERSION": str,
        "DPU": {
            "DPU_ID": str,
            "SITE_ID": str,
            "DPU_NAME": str,
            "DPU_LOCATION": str
        },
        "LOCAL_MQTT_BROKER": {
            "BROKER_IP_ADDRESS": str,
            "USERNAME": str,
            "PASSWORD": str,
            "PORT": int
        },
        "FWILD_MQTT_BROKER": {
            "BROKER_IP_ADDRESS": str,
            "USERNAME": str,
            "PASSWORD": str,
            "PORT": int
        },
        "CLOUD_MQTT_BROKER": {
            "BROKER_IP_ADDRESS": str,
            "USERNAME": str,
            "PASSWORD": str,
            "PORT": int,
            "CLIENT_ID": str
        }

    }

    def __init__(self):
        self.comment = None
        self.version = None
        self.dpu = None
        self.cloud = None
        self.lmb = None
        self.fwild_mb = None
        self.cmb = None
        self.alertMsgPhCfg = None
        self.json_data = None

    def read_cfg(self, file_name):
        if path.exists(file_name):
            with open(file_name) as f:
                try:
                    self.json_data = json.load(f)
                    Log.logger.info(f'Configuration File: {file_name} loaded successfully\n {self.json_data}')
                except json.JSONDecodeError as jex:
                    Log.logger.error(f'{file_name} does not have valid Json Config\n{jex}\n  Program terminated',
                                        exc_info=True)
                    sys.exit(2)
        else:
            Log.logger.error(f'{file_name} not found.  Program terminated')
            sys.exit(1)
        try:
            checker = Checker(WildCmConfRead.schema)
            result = checker.validate(self.json_data)
            Log.logger.info(f'{file_name} Checked OK. Result: {result}')
        except json_checker.core.exceptions.DictCheckerError as err:
            Log.logger.error(f'{file_name} is not valid {err}', exc_info=True)
            sys.exit(3)
        try:
            self.comment = self.json_data['COMMENT']
            self.version = self.json_data['VERSION']

            self.dpu = DpuStruct(**self.json_data['DPU'])
            self.lmb = LmbStruct(**self.json_data['LOCAL_MQTT_BROKER'])
            self.fwild_mb = LmbStruct(**self.json_data['FWILD_MQTT_BROKER'])
            self.cmb = CmbStruct(**self.json_data['CLOUD_MQTT_BROKER'])

            Log.logger.info(f'Configuration File: {file_name} Read successfully\n')
        except KeyError as jex:
            Log.logger.error(f'{file_name} do not have the data: {jex}', exc_info=True)
            sys.exit(3)


class DpuStruct(NamedTuple):
    DPU_ID: str
    SITE_ID: str
    DPU_NAME: str
    DPU_LOCATION: str


class LmbStruct(NamedTuple):
    BROKER_IP_ADDRESS: str
    USERNAME: str
    PASSWORD: str
    PORT: int


class CmbStruct(NamedTuple):
    BROKER_IP_ADDRESS: str
    USERNAME: str
    PASSWORD: str
    PORT: int
    CLIENT_ID: str


if __name__ == "__main__":
    if Log.logger is None:
        my_log = Log("CM")

    cfg = WildCmConfRead()
    cfg.read_cfg('../config/mvis_cm.conf')
    # get the interrogator - 0 channel 3 FBG sensor 2 values
    Log.logger.info('******************  In Main Program *******************')
    Log.logger.info(f'DPU: {cfg.dpu} \n')
    Log.logger.info(f'LOCAL MESSAGE BROKER: {cfg.lmb} \n')
    Log.logger.info(f'FWILD MESSAGE BROKER: {cfg.mvis_mb} \n')
    Log.logger.info(f'CLOUD MESSAGE BROKER: {cfg.cmb} \n')
    Log.logger.warning(f'Alert Message config: {cfg.alertMsgPhCfg}')
    for i in range (len(cfg.alertMsgPhCfg.TO_NOS)):
        Log.logger.warning(f'{i} th TO No: {cfg.alertMsgPhCfg.TO_NOS[i]}')
