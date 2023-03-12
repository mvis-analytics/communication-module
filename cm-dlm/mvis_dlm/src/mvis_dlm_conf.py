'''
*****************************************************************************
*File : mvis_dlm_conf.py
*Module : mvis_dlm
*Purpose : mvis data logging module (DLM) configuration class
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''import python packages'''
import json
import sys
from os import path
from typing import NamedTuple

import json_checker
from json_checker import Checker

'''Import WILD packages '''
sys.path.insert(1, "../../mvis_common")
from mvis_log import *
from MqttClient import *
from mvis_dlm_mqtt import *

class WildDlmConfRead:
    schema = {
        "COMMENT": str,
        "VERSION": str,
        "DPU_ID": str,
        "DPU_LOCATION": str,
        "DATABASE": {
                    "PROVIDER": str,
                    "USER": str,
                    "PASSWORD": str,
                    "HOST": str,
                    "DB_NAME": str
                    },
        "LOCAL_MESSAGE_BROKER": {
                    "BROKER_IP_ADDRESS": str,
                    "USERNAME": str,
                    "PASSWORD": str,
                    "PORT": int
        }
    }

    def __init__(self):
        self.comment = None
        self.version = None
        self.database = None
        self.json_data = None

    def read_cfg(self, file_name):
        if path.exists(file_name):
            with open(file_name) as f:
                try:
                    self.json_data = json.load(f)
                    Log.logger.info(f'Configuration File: {file_name} loaded successfully\n {self.json_data}')
                except json.JSONDecodeError as jex:
                    Log.logger.critical(f'{file_name} does not have valid Json Config\n{jex}\n  Program terminated')
                    sys.exit(2)
        else:
            Log.logger.critical(f'{file_name} not found.  Program terminated')
            sys.exit(1)
        try:
            checker = Checker(WildDlmConfRead.schema)
            result = checker.validate(self.json_data)
            Log.logger.info(f'{file_name} Checked OK. Result: {result}')
        except json_checker.core.exceptions.DictCheckerError as err:
            Log.logger.critical(f'{file_name} is not valid {err}')
            sys.exit(3)
        try:
            self.comment = self.json_data['COMMENT']
            self.version = self.json_data['VERSION']

            self.database = DatabaseStruct(**self.json_data['DATABASE'])
            Log.logger.info(f'Configuration File: {file_name} Read successfully\n')
        except KeyError as jex:
            Log.logger.critical(f'{file_name} do not have the data: {jex}')
            sys.exit(3)

class DatabaseStruct(NamedTuple):
    PROVIDER: str
    USER: str
    PASSWORD: str
    HOST: str
    DB_NAME: str

if __name__ == "__main__":
    if Log.logger is None:
      my_log = Log()

    cfg = WildDlmConfRead()
    
    cfg.read_cfg('../../config/mvis_dlm.conf')
    Log.logger.info('******************  In Main Program *******************')
    Log.logger.info(f'DATABASE: {cfg.database} \n')
