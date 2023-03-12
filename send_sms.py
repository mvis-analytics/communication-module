"""
*****************************************************************************
*File : mvis_sms.py
*Module : mvis_cm
*Purpose : mvis sms 
*Author : Sumankumar Panchal
*Copyright : Copyright 2022, Lab to Market Innovations Private Limited
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
import pysftp

# '''Import MVIS packages'''
from mvis_log import *
from MqttClient import MqttClient
from mvis_cm_conf import WildCmConfRead
from mvis_event_error_pub import EventErrorPub


import urllib.request
import urllib.parse

to_phone_numbers = '9663880102'
sender_name = 'LMRAIL'
textlocal_api_key = 'NTc0ZjU5NDE0ZTM3NTc2ODcxNDQ1NDY0Njg2ZTYyNTY='


sms_text2 = """Message: 1 of 1
 MVIS Alert Site: DFCC_DAQN 
 Train ID: T20221021171231 
 Entry Time: 21-10-2022 17:12:31 
 Direction: Down 
 Speed: 97.23 
 Total Axles: 238 
 Total MVIS Alerts: 312 
 . - L2MRail.com
"""


def train_consolidated_info(self, in_client, user_data, message):
    try:
        Log.logger.warning(f'train info: {message.payload}')
        topic_split = message.topic.split("/")
        if topic_split[-1] == 'train_consolidated_info':
            jmesg = json.loads(message.payload)
            sms_text = ' Message: 1 of 1' + \
                    '\n MVIS Alert Site: DFCC_DAQN' + \
                    '\n Train ID: ' + jmesg["train_id"] + \
                    '\n Entry Time: ' + entry_time + \
                    '\n Direction: ' + jmesg["direction"] + \
                    '\n Speed: ' + str(jmesg["train_speed"]) + \
                    '\n Total Axles: ' + str(total_axles) + \
                    '\n Total MVIS Alerts: ' + str(-) + \
                    '\n . - L2MRail.com'
        
            print("Message:")
            print(sms_text)
            resp = sendSMS(textlocal_api_key, to_phone_numbers, sender_name, sms_text)
            print(f'SMS Alert sent:\n{sms_text}\nResponse: \n{resp}')
        else:
            pass
    except Exception as ex:
        Log.logger.error(f'train_consolidated_info: exception: {ex}')

def sub_fwild_topic(fwild_client):
    """ Subscribe all required MQTT topics of Local MQTT broker"""
    try:
        fwild_client.sub("dpu_pm/train_consolidated_info", train_consolidated_info)
    except Exception as e:
        Log.logger.error(f'mvis_cm: sub_topic: exception: {e}', exc_info=True)

def sendSMS(apikey, numbers, sender, message):
    # fr = "Success"
    try:
        data = urllib.parse.urlencode({'apikey': apikey, 'numbers': numbers,
                                       'message': message, 'sender': sender})
        data = data.encode('utf-8')
        request = urllib.request.Request("https://api.textlocal.in/send/?")
        f = urllib.request.urlopen(request, data)
        fr = f.read()
    except Exception as ex:
        print(f'wild_cm: Unable to send SMS:\nException: {ex}')
        fr = "Failed"
    return fr


#if __name__ == "__main__":
#    print("Message:")
#    print(sms_text2)
    #resp = sendSMS(textlocal_api_key, to_phone_numbers, sender_name, sms_text)
#    print(f'SMS Alert (Right) sent:\n{sms_text}\nResponse: \n{resp}')


if __name__ == "__main__":
    if Log.logger is None:
        Log("CM-SMS")
    Log.logger.warning(
        f'\n************************************* CM Started *******************************************')

    '''read configuration'''
    cfg = WildCmConfRead()
    cfg.read_cfg('../config/mvis_cm.conf')

    fwild_client = MqttClient(cfg.fwild_mb.BROKER_IP_ADDRESS, cfg.fwild_mb.PORT, "mvis-cm-sms", cfg.fwild_mb.USERNAME, cfg.fwild_mb.PASSWORD,
                          'mvis-cm-sms')
    fwild_client.connect()

    sub_fwild_topic(fwild_client)

    while True:
        try:
            pass
        except KeyboardInterrupt:
            Log.logger.error(f'Keyboard Interrupt occurred. Exiting the program')
            sys.exit(0)
        except Exception as ex:
            Log.logger.error(f'Exception occurred. {ex}, exc_info=True')
