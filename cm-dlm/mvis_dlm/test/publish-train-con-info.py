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

count = 0

'''traid_id format = DDMMhhmm with T prefix'''
train_id = "T20200416104628"

'''dpu id number'''
dpu_id = "dpu_01"

'''axle_count is total number of axles of particular train.'''
total_axles = 100

'''entry time of 1st axle of train is train entry_time in wild measured zone.'''
train_entry_time = time.time()
train_exit_time = time.time()

'''total_wheels are twice the number of axles of the train'''
total_wheels = 200

'''No of bad wheel detected by system for a given train'''
total_bad_wheels = 2

'''Direction of train detected by wild system'''
direction = "towards station-A"

'''Average speed of full train'''
train_speed = 80.5

'''ILF warning and critical threshold '''
ilf_threshold_warning = 2.0
ilf_threshold_critical = 4.5

'''Dummy data ended'''



''' for each train one wildinfo mqtt msg will be publish by DPU through cloud MQTT broker'''
while True:
    ts = time.time()

    wild_per_train_info_msg = {"train_id" : train_id, "dpu_id" : dpu_id, "train_entry_time": train_entry_time, "train_exit_time": train_exit_time, "total_axles": total_axles,"total_wheels": total_wheels, "total_bad_wheels": total_bad_wheels, "direction": direction, "train_speed": train_speed, "ilf_threshold_warning": ilf_threshold_warning, "ilf_threshold_critical": ilf_threshold_critical}

    json_msg = json.dumps(wild_per_train_info_msg)

    client.publish("site_01/dpu_01/train_consolidated_info", json_msg)
    time.sleep(5)
