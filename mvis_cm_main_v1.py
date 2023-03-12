"""
*****************************************************************************
*File : mvis_cm_main.py
*Module : mvis_cm
*Purpose : mvis communication module main class
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*            Bangalore
*****************************************************************************
"""
# '''Import python packages'''
import json
import urllib.request
import urllib.parse

from datetime import datetime

import subprocess
import sys
import signal

# '''Import MVIS packages'''
from mvis_log import *
from MqttClient import MqttClient
from mvis_cm_conf import WildCmConfRead
from mvis_event_error_pub import EventErrorPub


class DpuCm:

    def __init__(self, cfg_obj, mq_client, cl_client, fwild_client, eve_err_pub_obj):
        Log.logger.info("DpuCM initialised")
        self.cfg = cfg_obj
        self.dpu_id = cfg_obj.dpu.DPU_ID
        self.site_id = cfg_obj.dpu.SITE_ID
        #self.cloud_topic_prefix = self.site_id + "/" + self.dpu_id
        self.cloud_topic_prefix = "mvis"
        self.event_msg_id = 0
        self.error_msg_id = 0
        self.ee_pub = eve_err_pub_obj
        self.mqtt_client = mq_client
        self.fwild_client = fwild_client
        self.cloud_mqtt_client = cl_client
        self.alert_msg = ''
        self.no_of_bad_wheels = 0
        self.location = cfg_obj.dpu.DPU_LOCATION

    def sub_fwild_topic(self):
        """ Subscribe all required MQTT topics of Local MQTT broker"""
        try:
            self.fwild_client.sub("dpu_pm/train_consolidated_info", self.train_info)
        except Exception as e:
            Log.logger.error(f'mvis_cm: sub_topic: exception: {e}', exc_info=True)

    def sub_mvis_topic(self):
        """ Subscribe all required MQTT topics of Local MQTT broker"""
        try:
            self.mqtt_client.sub("defect_info", self.defect_info)
        except Exception as e:
            Log.logger.error(f'mvis_cm: sub_topic: exception: {e}', exc_info=True)
   
    def defect_info(self, in_client, user_data, message):
        try:
            Log.logger.warning(f'defect info: {message.payload}')
        except Exception as ex:
            Log.logger.error(f'defect_info exception: {ex}')

    def train_info(self, in_client, user_data, message):
        try:
            Log.logger.warning(f'train info: {message.payload}')
        except Exception as ex:
            Log.logger.error(f'train_info exception: {ex}')

    def cloud_pub_topic(self, in_client, user_data, message):
        # Publish message to cloud Message broker
        try:
            topic_split = message.topic.split("/")
            if topic_split[-1] == 'train_consolidated_info':
                self.tci_alert_msg(message.payload)
            elif topic_split[-1] == 'train_processed_info':
                self.tpi_alert_msg(message.payload)
            cloud_topic = (self.cloud_topic_prefix + "/" + topic_split[1]).lower()

            self.cloud_mqtt_client.pub(cloud_topic, message.payload)
            Log.logger.warning(f'{cloud_topic} publish to cloud broker!!\n New payload : {message.payload}')
            # Log.logger.info(f'{cloud_topic} publish to cloud broker!!\n New payload : {message.payload}')
        except Exception as e:
            Log.logger.error(f'mvis_cm: cloud_pub_topic: exception: {e}', exc_info=True)

    def tci_alert_msg(self, jmsg=None):
        jmesg = json.loads(jmsg)
        self.no_of_bad_wheels = jmesg["total_bad_wheels"]
        if self.no_of_bad_wheels != 0:
            time_val = jmesg['train_entry_time']
            dtime = datetime.fromtimestamp(time_val)
            entry_time = dtime.strftime('%d-%m-%Y %H:%M:%S %Z')
            total_axles = jmesg["total_axles"]
            self.alert_msg = ' L2M-FWILD Alert Site: ' + self.location + \
                             '\n Train ID: ' + jmesg["train_id"] + \
                             '\n Entry Time: ' + entry_time + \
                             '\n Direction: ' + jmesg["direction"] + \
                             '\n Speed: ' + str(jmesg["train_speed"]) + \
                             '\n Total Axles: ' + str(total_axles) + \
                             '\n Total Unhealthy Wheels: ' + str(self.no_of_bad_wheels)
        else:
            self.alert_msg = ''
            self.no_of_bad_wheels = 0

    def cloud_sub_topic(self):
        """Cloud ILF Configuration topic Subscribe"""
        try:
            cloud_topic = (self.cloud_topic_prefix + "/" + "ilf_config").lower()
            Log.logger.warning(f'Cloud ILF config topic: {cloud_topic} subscribed')
            self.cloud_mqtt_client.sub(cloud_topic, self.pub_ilf_config_fn)
        except Exception as ex:
            Log.logger.error(f'mvis_cm: cloud_sub_topic: exception {ex}', exc_info=True)

    @staticmethod
    def signal_handler(signal, frame):
        Log.logger.info(f'Program Terminated by kill or ctrl-c signal: {signal}')
        sys.exit(0)


global Log

if __name__ == "__main__":
    if Log.logger is None:
        Log("CM")
    Log.logger.warning(
        f'\n************************************* CM Started *******************************************')

    '''read configuration'''
    cfg = WildCmConfRead()
    cfg.read_cfg('../config/mvis_cm.conf')

    m_client = MqttClient(cfg.lmb.BROKER_IP_ADDRESS, cfg.lmb.PORT, "mvis-cm-local", cfg.lmb.USERNAME, cfg.lmb.PASSWORD,
                          'mvis-dpu-cm')
    m_client.connect()

    fwild_client = MqttClient(cfg.fwild_mb.BROKER_IP_ADDRESS, cfg.fwild_mb.PORT, "mvis-cm-fwild", cfg.fwild_mb.USERNAME, cfg.fwild_mb.PASSWORD,
                          'mvis-cm-fwild')
    fwild_client.connect()
    
    c_client = MqttClient(cfg.cmb.BROKER_IP_ADDRESS, cfg.cmb.PORT, cfg.cmb.CLIENT_ID, cfg.cmb.USERNAME,
                          cfg.cmb.PASSWORD,
                          cfg.cmb.CLIENT_ID)
    c_client.connect()
    eve_err_pub = EventErrorPub(m_client, cfg.dpu.DPU_ID)

    '''create class object'''
    dpu_cm = DpuCm(cfg, m_client, c_client, fwild_client, eve_err_pub)

    signal.signal(signal.SIGINT, dpu_cm.signal_handler)

    '''subscribe topics'''
    dpu_cm.sub_fwild_topic()
    dpu_cm.sub_mvis_topic()

    '''subscribe cloud topic'''
    #dpu_cm.cloud_sub_topic()

    conn_ok = False
    prog_start = True
    disconnect_count = 0
    check_timer = 10
    resp_str = ''
    if sys.platform.startswith('win32'):
        ping_cmd = ['ping', '-n', '3', '8.8.8.8']
        resp_str = '(0% loss)'
    elif sys.platform.startswith('linux'):
        ping_cmd = ['ping', '-q', '-c', '3', '8.8.8.8']
        resp_str = ', 0% packet loss'
    while True:
        try:
            ''' every 3 minutes, check communication '''
            time.sleep(check_timer)
            res = subprocess.Popen(ping_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            outmsg = res.stdout.read().decode()
            errmsg = res.stderr.read().decode()
            if errmsg == '' and (outmsg.find(resp_str) != -1):
                Log.logger.warning('Ping Success: Internet Connectivity OK')
                disconnect_count = 0
                check_timer = 180
                if not conn_ok:
                    Log.logger.warning("Internet Communication Successful")
                    eve_err_pub.publish_event_info("cm", "CM-EVENT-001", "Internet Communication Successful")
                    conn_ok = True
                prog_start = False
            else:
                Log.logger.warning(f'ping failed- Ping output\n{outmsg}\nError Msg:\n {errmsg}')
                check_timer = 10
                disconnect_count = disconnect_count + 1
                if (prog_start or conn_ok) and disconnect_count > 2:
                    Log.logger.warning(f"CM-ERROR-001: No response from Server")
                    eve_err_pub.publish_error_info("cm", "CM-ERROR-001", EventErrorPub.WARNING,
                                                   "CM-ERROR-01: No response from server. Check Internet link")
                    conn_ok = False
                    disconnect_count = 0
                    prog_start = False
        except KeyboardInterrupt:
            Log.logger.error(f'Keyboard Interrupt occurred. Exiting the program')
            sys.exit(0)
        except Exception as ex:
            Log.logger.error(f'Exception occurred. {ex}, exc_info=True')

