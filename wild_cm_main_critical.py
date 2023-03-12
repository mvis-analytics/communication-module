"""
*****************************************************************************
*File : wild_cm.py
*Module : wild_cm
*Purpose : wild communication module main class
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*            Bangalore
*****************************************************************************
"""
# '''Import python packages'''
import json

from twilio.rest import Client
from datetime import datetime

import subprocess
import sys
import signal

# '''Import WILD packages'''
from wild_log import *
from MqttClient import MqttClient
from wild_cm_conf import WildCmConfRead
from wild_event_error_pub import EventErrorPub


class DpuCm:

    def __init__(self, cfg_obj, mq_client, cl_client, eve_err_pub_obj):
        Log.logger.info("DpuCM initialised")
        self.cfg = cfg_obj
        self.dpu_id = cfg_obj.dpu.DPU_ID
        self.site_id = cfg_obj.dpu.SITE_ID
        self.cloud_topic_prefix = self.site_id + "/" + self.dpu_id
        self.event_msg_id = 0
        self.error_msg_id = 0
        self.ee_pub = eve_err_pub_obj
        self.mqtt_client = mq_client
        self.cloud_mqtt_client = cl_client
        self.alert_msg = ''
        self.no_of_engine = 0
        self.no_of_coaches = 0

    def check_internet_link(self):
        conn_ok = False
        while True:
            try:
                res = 0
                if sys.platform.startswith('win32'):
                    res = subprocess.call(['ping', '-n', '3', '8.8.8.8'])
                elif sys.platform.startswith('linux'):
                    res = subprocess.call(['ping', '-c', '3', '8.8.8.8'])
                if res == 0:
                    if not conn_ok:
                        Log.logger.info("Internet Communication Successful")
                        self.ee_pub.publish_event_info("cm", "CM-EVENT-001", "Internet Communication Successful")
                        conn_ok = True
                    else:
                        pass
                elif res == 2:
                    if conn_ok:
                        Log.logger.warning("CM-ERROR-001: No response from Server")
                        self.ee_pub.publish_error_info("cm", "CM-ERROR-001", EventErrorPub.WARNING,
                                                       "CM-ERROR-01: No response from server. Check Internet link")
                        conn_ok = False
                else:
                    if conn_ok:
                        Log.logger.warning("CM-ERROR-002: Communication failed with Internet")
                        self.ee_pub.publish_error_info("cm", "CM-ERROR-002", EventErrorPub.WARNING,
                                                       "CM-ERROR-02: Communication failed.  Check Internet link")
                        conn_ok = False
                ''' every 3 minutes, check communication '''
                time.sleep(60 * 3)
            except Exception as e:
                Log.logger.warning(f'wild_cm_main: check_internet_link: exception: {e}')

    def sub_topic(self):
        """ Subscribe all required MQTT topics of Local MQTT broker"""
        try:
            self.mqtt_client.sub("dpu_pm/train_processed_info", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_pm/train_consolidated_info", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_dam/health_info", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_pm/events", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_pm/errors", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_dam/events", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_dam/errors", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_tdfm/events", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_tdfm/errors", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_dlm/events", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_dlm/errors", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_cm/events", self.cloud_pub_topic)
            self.mqtt_client.sub("dpu_cm/errors", self.cloud_pub_topic)
        except Exception as e:
            Log.logger.critical(f'wild_cm: sub_topic: exception: {e}', exc_info=True)

    def cloud_pub_topic(self, in_client, user_data, message):
        """ Publish message to cloud Message broker """
        try:
            topic_split = message.topic.split("/")
            self.send_alert_msg(topic_split[-1], message.payload)
            cloud_topic = (self.cloud_topic_prefix + "/" + topic_split[1]).lower()

            # '''add dpu_id in the received message before publish to cloud Message broker'''
            # self.decoded_message = json.loads(message.payload)
            # self.decoded_message['dpu_id'] = self.dpu_id
            # self.new_encoded_message = json.dumps(self.decoded_message)

            self.cloud_mqtt_client.pub(cloud_topic, message.payload)
            Log.logger.info(f'{cloud_topic} publish to cloud broker!!\n New payload : {message.payload}')
        except Exception as e:
            Log.logger.critical(f'wild_cm: cloud_pub_topic: exception: {e}', exc_info=True)

    def send_alert_msg(self, topic, mqtt_msg=None):

        if topic == 'train_consolidated_info':
            jmesg = json.loads(mqtt_msg)
            no_of_bad_wheels = jmesg["total_bad_wheels"]
            if no_of_bad_wheels != 0:
                self.no_of_engine = 1
                self.no_of_coaches = 0
                time_val = jmesg['train_entry_time']
                dtime = datetime.fromtimestamp(time_val)
                time_str = dtime.strftime('%d/%m/%Y %H:%M:%S %Z')
                total_axles = jmesg["total_axles"]
                if ((total_axles - 6) - int((total_axles - 6) / 4) * 4) != 0:
                    self.no_of_engine = self.no_of_engine + 1
                if total_axles != (self.no_of_engine * 6):
                    self.no_of_coaches = int(((total_axles - (self.no_of_engine * 6)) / 4))
                self.alert_msg = "***** L2m-FWILD *****\nAlert from Site: LC-15" + \
                                 "\nTrain ID: " + jmesg["train_id"] + \
                                 '\nEntry Time: ' + time_str + \
                                 '\nDirection: ' + jmesg["direction"] + \
                                 '\nSpeed: ' + str(jmesg["train_speed"]) + \
                                 '\nTotal Axles: ' + str(total_axles) + \
                                 ' Engine: ' + str(self.no_of_engine) + \
                                 ' Coaches: ' + str(self.no_of_coaches) + \
                                 '\nTotal Unhealthy Wheel: ' + str(no_of_bad_wheels)
            else:
                self.alert_msg = ''
                self.no_of_engine = 0
                self.no_of_coaches = 0
        elif topic == 'train_processed_info':
            if self.alert_msg == '':
                return
            jmesg = json.loads(mqtt_msg)
            wheel_status_left = jmesg["wheel_status_left"]
            wheel_status_right = jmesg["wheel_status_right"]
            send_alert = False
            alert_tp_msg = ''
            coach_no = 1
            engine_no = 1
            wheel_no = 1
            for i in range(0, len(wheel_status_left)):
                if (wheel_status_left[i] == 3) or (wheel_status_right[i] == 3):
                    send_alert = True
                    alert_tp_msg = alert_tp_msg + '\n\nAxle # ' + str(i + 1)
                    if i < (self.no_of_engine * 6):
                        alert_tp_msg = alert_tp_msg + ' Engine # ' + str(engine_no) + \
                                       ' Wheel # ' + str(wheel_no)
                    else:
                        alert_tp_msg = alert_tp_msg + ' Coach # ' + str(coach_no) + \
                                       ' Wheel # ' + str(wheel_no)

                    if wheel_status_left[i] == 3:
                        alert_tp_msg = alert_tp_msg + '\nSide: East\nAlert Level: '
                        alert_tp_msg = alert_tp_msg + 'Critical'
                    if wheel_status_right[i] == 3:
                        alert_tp_msg = alert_tp_msg + '\nSide: West\nAlert Level: '
                        alert_tp_msg = alert_tp_msg + 'Critical'
                    alert_tp_msg = alert_tp_msg + '\nEMDIL: ' + str(jmesg["max_dyn_load_left"][i]) + \
                                   '\nEILF: ' + str(jmesg["ilf_left"][i]) + \
                                   '\nWMDIL: ' + str(jmesg["max_dyn_load_right"][i]) + \
                                   '\nWILF: ' + str(jmesg["ilf_right"][i])
                if (i + 1) < (self.no_of_engine * 6):
                    if wheel_no == 6:
                        engine_no = engine_no + 1
                        wheel_no = 1
                    else:
                        wheel_no = wheel_no + 1
                elif (i + 1) == (self.no_of_engine * 6):
                    coach_no = 1
                    wheel_no = 1
                else:
                    if wheel_no == 4:
                        coach_no = coach_no + 1
                        wheel_no = 1
                    else:
                        wheel_no = wheel_no + 1
            if send_alert is True:
                self.alert_msg = self.alert_msg + alert_tp_msg
                print(self.alert_msg)
                account_sid = 'AC0c7fb127fea4867369fb55aad833adff'
                auth_token = '11a241653bbfb04da0829bb6b7142119'
                client = Client(account_sid, auth_token)
                # message = client.messages.create(
                #     from_='whatsapp:+14155238886',
                #     body=self.alert_msg,
                #     to='whatsapp:+919901251345'
                # )
                for i in range(len(cfg.alertMsgPhCfg.TO_NOS)):
                    message = client.messages.create(
                        body=self.alert_msg,
                        from_='+17739857719',
                        to=cfg.alertMsgPhCfg.TO_NOS[i]
                    )
                    print(message.sid)
            self.alert_msg = ''
            self.no_of_engine = 0
            self.no_of_coaches = 0
        else:
            return

    def cloud_sub_topic(self):
        """Cloud ILF Configuration topic Subscribe"""
        try:
            cloud_topic = (self.cloud_topic_prefix + "/" + "ilf_config").lower()
            Log.logger.warning(f'Cloud ILF config topic: {cloud_topic} subscribed')
            self.cloud_mqtt_client.sub(cloud_topic, self.pub_ilf_config_fn)
        except Exception as ex:
            Log.logger.critical(f'wild_cm: cloud_sub_topic: exception {ex}', exc_info=True)

    def pub_ilf_config_fn(self, in_client, user_data, message):
        """Publish ILF Configuration locally"""
        try:
            dpu_topic = "dpu_cm/ilf_config"
            self.mqtt_client.pub(dpu_topic, message.payload)
            Log.logger.warning(f'{dpu_topic} publish to local broker!!\n Message payload : {message.payload}')
        except Exception as ex:
            Log.logger.critical(f'wild_cm : pub_ilf_config_fn: exception {ex}', exc_info=True)

    @staticmethod
    def signal_handler(signal, frame):
        Log.logger.info(f'Program Terminated by kill or ctrl-c signal: {signal}')
        sys.exit(0)


if __name__ == "__main__":
    if Log.logger is None:
        Log("CM")
    Log.logger.info("wild_cm: started..")

    '''read configuration'''
    cfg = WildCmConfRead()
    cfg.read_cfg('../config/wild_cm.conf')

    m_client = MqttClient(cfg.lmb.BROKER_IP_ADDRESS, cfg.lmb.PORT, "cm-local", cfg.lmb.USERNAME, cfg.lmb.PASSWORD,
                          'dpu-cm')
    m_client.connect()

    c_client = MqttClient(cfg.cmb.BROKER_IP_ADDRESS, cfg.cmb.PORT, cfg.cmb.CLIENT_ID, cfg.cmb.USERNAME, cfg.cmb.PASSWORD,
                          cfg.cmb.CLIENT_ID)
    c_client.connect()
    eve_err_pub = EventErrorPub(m_client, cfg.dpu.DPU_ID)

    '''create class object'''
    dpu_cm = DpuCm(cfg, m_client, c_client, eve_err_pub)

    signal.signal(signal.SIGINT, dpu_cm.signal_handler)

    '''subscribe topics'''
    dpu_cm.sub_topic()

    '''subscribe cloud topic'''
    dpu_cm.cloud_sub_topic()

    '''check internet connection periodically'''
    # t1 = threading.Timer(10, dpu_cm.check_internet_link)
    t1 = threading.Thread(target=dpu_cm.check_internet_link, args=())
    t1.daemon = True
    t1.start()
    time.sleep(0.001)
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        Log.logger.critical(f'Keyboard Interrupt occurred. Exiting the program')
    t1.join()
    sys.exit(0)
