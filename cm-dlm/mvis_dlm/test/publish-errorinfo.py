"""
Copyright © 2020, Lab to market innovations private limited, Bengaluru. 
All rights reserved.

Author: Sumankumar Panchal
"""

'''Test script for JSON message'''
'''    ts = FloatField()
    dpu_id = CharField()
    msg_id = IntegerField(null = True)
    error_id = CharField(null = True)
    error_severity = IntegerField(null = True)
    error_desc = TextField(null = True)
'''

import os
import sys
import time
import json
import numpy as np
import datetime

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

def on_connect(client, userdata, flags,rc):
    print("connected")

def on_message(client, userdata, msg):
    print(msg.payload)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect("127.0.0.1", 1883, 60)

'''Dummy data started'''
dpu_id = "DPU_01"
msg_id = 1
error_id = "DLM-ERROR-001"
error_severity = 3
error_desc = "DLM: ERROR: XYZ"
'''Dummy data ended'''

while True:
    msg_id = msg_id + 1
    ts = time.time()
    wild_error_msg = {"ts" : ts, "dpu_id": dpu_id, "msg_id": msg_id, "error_id" : error_id, "error_severity": error_severity, "error_desc": error_desc}
    json_msg = json.dumps(wild_error_msg)
    client.publish("site_01/dpu_01/errors", json_msg)
    time.sleep(10)

sys.exit(0)
