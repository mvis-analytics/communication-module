'''
*****************************************************************************
*File : mvis_dlm_conf.py
*Module : mvis_dlm
*Purpose : mvis data logging module (DLM) MQTT publish EVENT/ERROR class
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''import python packages '''
import sys
import json

'''import mvis packages'''
sys.path.insert(1, "../../mvis_common")
from mvis_log import *
from MqttClient import *

class DlmPub:
    '''Publish DLM Events and Errors'''
    def __init__(self):
        pass

    #def publish_error_info(self, mqtt_client, dpu_id, msg_id, msg_error_id, msg_error_severity, msg_error_desc):
    def publish_error_info(self, mqtt_client, msg_error_id, msg_error_severity, msg_error_desc):
        '''
        JSON {
            “ts”: <UTC time>,
            “msg_id”: <int>,
            "dpu_id": <string>,
            “error_id”: <string>, # e.g .PM-ERROR-001
            “error_severity”: <int>, # e.g. 1 = “Info” or 2 = “Warning” or 3 = “Critical”
            “error_desc”: <string>
        }
        '''
        try:
            self.mqtt_client = mqtt_client
            self.error_ts = time.time()
            #self.error_dpu_id = str(dpu_id)
            #self.error_msg_id = int(msg_id)
            self.error_id = str(msg_error_id)
            self.error_severity = int(msg_error_severity)
            self.error_desc = str(msg_error_desc)
            #self.error_msg = {"ts" : self.error_ts, "dpu_id": self.error_dpu_id, "msg_id": self.error_msg_id, "error_id" : self.error_id, "error_severity": self.error_severity, "error_desc": self.error_desc}
            self.error_msg = {"ts" : self.error_ts, "error_id" : self.error_id, "error_severity": self.error_severity, "error_desc": self.error_desc}
            self.error_json_msg = json.dumps(self.error_msg)
            self.mqtt_client.pub("dpu_dlm/errors", self.error_json_msg)
            Log.logger.info(f'publish_error_info: {self.error_json_msg}')
        except Exception as e:
            Log.logger.critical(f'mvis_dlm_conf: publish_error_info: {e}')

    def publish_event_info(self, mqtt_client, dpu_id, msg_id, msg_event_id, msg_event_desc):
        '''
        JSON {
            “ts”: <UTC time>,
            "dpu_id": <string>,
            “msg_id”: <int>,
            “event_id”: <string>, # e.g .PM-EVENT-001
            “event_desc”: <string>
        }
        '''
        try:
            self.mqtt_client = mqtt_client
            self.event_ts = time.time()
            self.event_dpu_id = str(dpu_id)
            self.event_msg_id = int(msg_id)
            self.event_id = str(msg_event_id)
            self.event_desc = str(msg_event_desc)
            self.event_msg = {"ts": self.event_ts, "dpu_id": self.event_dpu_id,"msg_id": self.event_msg_id, "event_id" : self.event_id, "event_desc": self.event_desc}
            self.event_json_msg = json.dumps(self.event_msg)
            self.mqtt_client.pub("dpu_dlm/error_info", self.event_json_msg)
            Log.logger.info(f'publish_error_info: {self.event_json_msg}')
        except Exception as e:
            Log.logger.critical(f'mvis_dlm_conf: publish_event_info: {e}')


if __name__ == "__main__":
    if Log.logger is None:
      my_log = Log()

    Log.logger.info('******************  In Main Program *******************')
