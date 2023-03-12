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
''' communication link '''
comm_link = "up"

'''interrogator_link: up/down/unknown '''
interrogator_link = "down"

'''dpu_id'''
dpu_id = "DPU_01"

count = 0
''' Health status of Interrogator link, Cloud link with DPU will be publish by DPU through cloud MQTT broker periodically and when it detects changes in link status'''
while True:
    ts = time.time()
    wild_health_msg = {"ts": str(ts), "dpu_id": dpu_id, "comm_link": comm_link , "interrogator_link": interrogator_link, "S1": "up", "S2": "up", "S3": "down", "S4": "up", "S5": "down", "S6" : "up", "S7":"up", "S8":"down", "S9":"down", "S10":"up", "S11":"down", "S12":"up", "V1":"down","V2":"up","V3":"up", "V4":"down", "L1": "up", "L2": "down", "L3": "up", "L4":"up", "T1": "up", "T2": "up", "T3": "up", "T4":"up"}

    json_msg = json.dumps(wild_health_msg)
    client.publish("site_01/dpu_01/health_info", json_msg)
    count += 1
    print("count:", count)
    print("publish: site_01/dpu_01/health_info", json_msg)
    time.sleep(5)

    if count > 10:
        sys.exit(0)
