'''
*****************************************************************************
*File : mvis_dlm_main.py
*Module : mvis_dlm
*Purpose : mvis data logging module main class
*Author : Sumankumar Panchal
*Copyright : Copyright 2022, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python packages'''
import sys

'''Import WILD packages '''
sys.path.insert(1, "../../mvis_common")
from mvis_log import *
from mvis_model import *
from mvis_api import WildAPI
from mvis_dlm_conf import *
from MqttClient import *


class DLMSub:
    ''' DLM MQTT Subscribe class / methods '''
    def __init__(self):
        self.mvis_api = WildAPI(cfg, mqtt_api_client)

    def dpu_pm_tpd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tpd_sub_fn : {message.payload}')
        self.mvis_api.insert_train_processed_info(message.payload)

    def dpu_pm_tcd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tcd_sub_fn : {message.payload}')
        self.mvis_api.insert_train_consolidated_info(message.payload)

    def dpu_event_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_event_sub_fn : {message.payload}')
        self.mvis_api.insert_mvis_event_info(message.payload)

    def dpu_error_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_error_sub_fn : {message.payload}')
        self.mvis_api.insert_mvis_error_info(message.payload)

    def dpu_health_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_health_sub_fn : {message.payload}')
        self.mvis_api.insert_mvis_health_info(message.payload)

    def as_processed_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'as_processed_sub_fn : {message.payload}')
        self.mvis_api.insert_mvis_processed_info(message.payload)
        self.mvis_api.insert_mvis_wagon_info(message.payload)

if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log()

    '''read configuration file'''
    cfg = WildDlmConfRead()
    cfg.read_cfg('../../config/mvis_dlm.conf')

    '''Create MQTT Client object and connect '''
    broker_ip = cfg.json_data["LOCAL_MESSAGE_BROKER"]["BROKER_IP_ADDRESS"]
    broker_port = cfg.json_data["LOCAL_MESSAGE_BROKER"]["PORT"]
    broker_username = cfg.json_data["LOCAL_MESSAGE_BROKER"]["USERNAME"]
    broker_password = cfg.json_data["LOCAL_MESSAGE_BROKER"]["PASSWORD"]
    
    mqtt_client = MqttClient(broker_ip, broker_port, "mvis-dlm-main", broker_username, broker_password, 'mvis-dlm-main-client')
    mqtt_client.connect()

    mqtt_api_client = MqttClient(broker_ip, broker_port, "mvis-dlm-api", broker_username, broker_password, 'mvis-dlm-api-client')
    mqtt_api_client.connect()

    '''initialise mvis_api and connect database'''
    db_api = WildAPI(cfg, mqtt_api_client)
    psql_db = db_api.connect_database(cfg)

    '''Create database model'''
    if psql_db:
        psql_db.create_tables([TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo])

    '''Create DLMSub class object'''
    dlm_sub = DLMSub()

    '''Subscribe all required MQTT topics '''
    mqtt_client.sub("site_01/dpu_01/train_processed_info", dlm_sub.dpu_pm_tpd_sub_fn)
    mqtt_client.sub("site_01/dpu_01/train_consolidated_info", dlm_sub.dpu_pm_tcd_sub_fn)
    mqtt_client.sub("site_01/dpu_01/events", dlm_sub.dpu_event_sub_fn)
    mqtt_client.sub("site_01/dpu_01/errors", dlm_sub.dpu_error_sub_fn)
    mqtt_client.sub("site_01/dpu_01/health_info", dlm_sub.dpu_health_sub_fn)
    mqtt_client.sub("site_01/as_01/mvis_processed_info", dlm_sub.as_processed_sub_fn)
    
    #mqtt_client.sub("site_03/dpu_03/train_processed_info", dlm_sub.dpu_pm_tpd_sub_fn)
    #mqtt_client.sub("site_03/dpu_03/train_consolidated_info", dlm_sub.dpu_pm_tcd_sub_fn)
    #mqtt_client.sub("site_03/dpu_03/events", dlm_sub.dpu_event_sub_fn)
    #mqtt_client.sub("site_03/dpu_03/errors", dlm_sub.dpu_error_sub_fn)
    #mqtt_client.sub("site_03/dpu_03/health_info", dlm_sub.dpu_health_sub_fn)

    while True:
        pass
    sys.exit(0)
