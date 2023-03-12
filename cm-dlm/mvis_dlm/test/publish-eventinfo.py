"""
Copyright © 2020, Lab to market innovations private limited, Bengaluru. 
All rights reserved.

Author: Sumankumar Panchal
"""

'''Test script for JSON message'''
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
msg_id = 0
dpu_id = "DPU_01"
event_id = "DLM-EVENT-001"
event_desc = "DLM: EVENT-001: XYZ"
'''Dummy data ended'''

while True:
    msg_id = msg_id + 1
    ts = time.time()
    wild_event_msg = {"ts": ts, "dpu_id":dpu_id, "msg_id": msg_id, "event_id" : event_id, "event_desc": event_desc}
    json_msg = json.dumps(wild_event_msg)
    client.publish("site_01/dpu_01/events", json_msg)
    time.sleep(10)

sys.exit(0)
