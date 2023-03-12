'''
*****************************************************************************
*File : mvis_sms_service.py
*Module : mvis_cm
*Purpose : mvis sms service 
*Author : Sumankumar Panchal
*Copyright : Copyright 2022, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python packages'''
import sys

'''Import WILD packages '''
sys.path.insert(1, "../../mvis_common")
from mvis_sms_log import *
from mvis_cm_conf import *
from MqttClient import *


import urllib.request
import urllib.parse

to_phone_numbers = '9663880102, 9449188006, 9880708896'
sender_name = 'LMRAIL'
textlocal_api_key = 'NTc0ZjU5NDE0ZTM3NTc2ODcxNDQ1NDY0Njg2ZTYyNTY='

class MVISSub:
    ''' Fwild train consolidated info MQTT Subscribe class / methods '''
    def __init__(self):
        pass

    def dpu_pm_tcd_sub_fn(self, in_client, user_data, message):
        Log.logger.info(f'dpu_pm_tcd_sub_fn : {message.payload}')
        #self.mvis_api.insert_train_consolidated_info(message.payload)
        print(f'train consolidated info')
        Log.logger.warning(f'train info: {message.payload}')
        topic_split = message.topic.split("/")
        if topic_split[-1] == 'train_consolidated_info':
            jmesg = json.loads(message.payload)
            entry_time = time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(jmesg["train_entry_time"]))
            sms_text = ' Message: 1 of 1' + \
                    '\n MVIS Alert Site: DFCC_DAQN' + \
                    '\n Train ID: ' + str(jmesg["train_id"]) + \
                    '\n Entry Time: ' + str(entry_time) + \
                    '\n Direction: ' + str(jmesg["direction"]) + \
                    '\n Speed: ' + str(jmesg["train_speed"])+'km/h' + \
                    '\n Total Axles: ' + str(jmesg["total_axles"]) + \
                    '\n Total MVIS Alerts: -' + \
                    '\n . - L2MRail.com'

            Log.logger.info(f'SMS TEXT: {sms_text}')
   
            resp = self.sendSMS(textlocal_api_key, to_phone_numbers, sender_name, sms_text)
            print(f'SMS Alert sent:\n{sms_text}\nResponse: \n{resp}')

    def sendSMS(self, apikey, numbers, sender, message):
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

if __name__ == '__main__':
    if Log.logger is None:
        my_log = Log('sms')

    '''read configuration'''
    cfg = WildCmConfRead()
    cfg.read_cfg('../config/mvis_cm.conf')

    mqtt_client = MqttClient(cfg.fwild_mb.BROKER_IP_ADDRESS, cfg.fwild_mb.PORT, "mvis-cm-sms", cfg.fwild_mb.USERNAME, cfg.fwild_mb.PASSWORD,
                          'mvis-cm-sms')
    mqtt_client.connect()

    '''Create MVISSub class object'''
    mvis_sub = MVISSub()

    '''Subscribe all required MQTT topics '''
    mqtt_client.sub("dpu_pm/train_consolidated_info", mvis_sub.dpu_pm_tcd_sub_fn)

    while True:
        pass
    sys.exit(0)
