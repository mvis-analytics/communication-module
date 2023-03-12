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
train_id = "T08041101"
'''Line id as per sidings'''
line_id = "LTS-1" 

'''axle_id is number of axles of particular train. It will start from 0 for every new train'''
axle_id1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
axle_id2 = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
axle_id3 = [21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
axle_id4 = [31, 32, 33, 34, 35, 36, 37, 38, 39, 40]
axle_id5 = [41, 42, 43, 44, 45, 46, 47, 48, 49, 50]
axle_id = axle_id1 + axle_id2 + axle_id3 + axle_id4 + axle_id5

'''
Avg Dynamic Load 5 - 9 Tonne
Max Dynamic Load - This can be any value more than 5. 
max value depends upon severity of defect i.e. 25 Tonne
'''

''' one loco and one coach '''
avg_dyn_load_left1 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_left2 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_left3 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_left4 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_left5 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_right1 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_right2 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_right3 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_right4 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]
avg_dyn_load_right5 = [8.5, 8.6, 8.9, 8.4, 8.7, 9.0, 6.0, 5.9, 6.1, 6.2]

avg_dyn_load_left = avg_dyn_load_left1 + avg_dyn_load_left2 + avg_dyn_load_left3 + avg_dyn_load_left4 + avg_dyn_load_left5
avg_dyn_load_right = avg_dyn_load_right1 + avg_dyn_load_right2 + avg_dyn_load_right3 + avg_dyn_load_right4 + avg_dyn_load_right5

''' one loco and one coach '''
max_dyn_load_left1 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 12.0, 6.1, 6.2, 6.3] 
max_dyn_load_left2 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 12.0, 6.1, 6.2, 6.3] 
max_dyn_load_left3 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 12.0, 6.1, 6.2, 6.3] 
max_dyn_load_left4 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 12.0, 6.1, 6.2, 6.3] 
max_dyn_load_left5 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 12.0, 6.1, 6.2, 6.3] 

max_dyn_load_right1 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 6.2, 6.1, 6.2, 6.3] 
max_dyn_load_right2 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 6.2, 6.1, 6.2, 6.3] 
max_dyn_load_right3 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 6.2, 6.1, 6.2, 6.3] 
max_dyn_load_right4 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 6.2, 6.1, 6.2, 6.3] 
max_dyn_load_right5 = [9.0, 8.7, 9.1, 8.9, 8.9, 9.0, 6.2, 6.1, 6.2, 6.3] 
max_dyn_load_left = max_dyn_load_left1 + max_dyn_load_left2 + max_dyn_load_left3 + max_dyn_load_left4 + max_dyn_load_left5 
max_dyn_load_right = max_dyn_load_right1 + max_dyn_load_right2 + max_dyn_load_right3 + max_dyn_load_right4 + max_dyn_load_right5 

'''Vertical Load -  5 to 9 Tonne (When wheel defect hits it can be even 2-3 times more)'''
vertical_load_left1 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 10.0, 7.0, 7.1, 7.2]
vertical_load_left2 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 10.0, 7.0, 7.1, 7.2]
vertical_load_left3 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 10.0, 7.0, 7.1, 7.2]
vertical_load_left4 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 10.0, 7.0, 7.1, 7.2]
vertical_load_left5 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 10.0, 7.0, 7.1, 7.2]

vertical_load_right1 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 7.9, 7.0, 7.1, 7.2]
vertical_load_right2 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 7.9, 7.0, 7.1, 7.2]
vertical_load_right3 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 7.9, 7.0, 7.1, 7.2]
vertical_load_right4 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 7.9, 7.0, 7.1, 7.2]
vertical_load_right5 = [7.0, 6.6, 7.1, 7.7, 6.8, 7.0, 7.9, 7.0, 7.1, 7.2]

vertical_load_left = vertical_load_left1 + vertical_load_left2 + vertical_load_left3 + vertical_load_left4 + vertical_load_left5
vertical_load_right = vertical_load_right1 + vertical_load_right2 + vertical_load_right3 + vertical_load_right4 + vertical_load_right5

'''Lateral Load  - 2 to 5 Tonne  (When wheel defect hits it could be more)'''
lateral_load_left1 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 4.1, 2.0, 2.2, 2.1]
lateral_load_left2 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 4.1, 2.0, 2.2, 2.1]
lateral_load_left3 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 4.1, 2.0, 2.2, 2.1]
lateral_load_left4 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 4.1, 2.0, 2.2, 2.1]
lateral_load_left5 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 4.1, 2.0, 2.2, 2.1]

lateral_load_left = lateral_load_left1 + lateral_load_left2 + lateral_load_left3 + lateral_load_left4 + lateral_load_left5

lateral_load_right1 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 2.1, 2.0, 2.2, 2.1]
lateral_load_right2 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 2.1, 2.0, 2.2, 2.1]
lateral_load_right3 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 2.1, 2.0, 2.2, 2.1]
lateral_load_right4 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 2.1, 2.0, 2.2, 2.1]
lateral_load_right5 = [2.0, 2.1, 2.0, 2.2, 2.1, 2.0, 2.1, 2.0, 2.2, 2.1]

lateral_load_right = lateral_load_right1 + lateral_load_right2 + lateral_load_right3 + lateral_load_right4 + lateral_load_right5

'''
ILF factor Range - As per APNA Tech WILD. But this is a variable value 
Maintenance Alarm  -  2< ILF<4.5
Critical Alarm - > 4.5
'''
ilf_left1 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 3.5, 1.5, 1.4, 1.3] 
ilf_left2 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 3.5, 1.5, 1.4, 1.3] 
ilf_left3 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 3.5, 1.5, 1.4, 1.3] 
ilf_left4 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 3.5, 1.5, 1.4, 1.3] 
ilf_left5 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 3.5, 1.5, 1.4, 1.3] 

ilf_left = ilf_left1 + ilf_left2 + ilf_left3 + ilf_left4 + ilf_left5 

ilf_right1 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 1.8, 1.5, 1.4, 1.3] 
ilf_right2 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 1.8, 1.5, 1.4, 1.3] 
ilf_right3 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 1.8, 1.5, 1.4, 1.3] 
ilf_right4 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 1.8, 1.5, 1.4, 1.3] 
ilf_right5 = [1.0, 1.2, 1.5, 1.6, 1.9, 1.0, 1.8, 1.5, 1.4, 1.3] 

ilf_right = ilf_right1 + ilf_right2 + ilf_right3 + ilf_right4 + ilf_right5

axle_speed1 = [80, 80.1, 80.1, 79.5, 79.9, 79.8, 80.5, 80.2, 81, 80.6] 
axle_speed2 = [80, 80.1, 80.1, 79.5, 79.9, 79.8, 80.5, 80.2, 81, 80.6] 
axle_speed3 = [80, 80.1, 80.1, 79.5, 79.9, 79.8, 80.5, 80.2, 81, 80.6] 
axle_speed4 = [80, 80.1, 80.1, 79.5, 79.9, 79.8, 80.5, 80.2, 81, 80.6] 
axle_speed5 = [80, 80.1, 80.1, 79.5, 79.9, 79.8, 80.5, 80.2, 81, 80.6] 

axle_speed = axle_speed1 + axle_speed2 + axle_speed3 + axle_speed4 + axle_speed5 

'''
status = 0 : No defect
status = 1 : defect
'''
status1 = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1] 
status2 = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1] 
status3 = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1] 
status4 = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1] 
status5 = [1, 1, 1, 1, 1, 1, 2, 1, 1, 1] 
wheel_status_left =  status1 + status2 + status3 + status4 + status5 
wheel_status_right =  status1 + status2 + status3 + status4 + status5 

'''Dummy data ended'''


''' for each train one wildinfo mqtt msg will be publish by DPU through cloud MQTT broker'''
while True:
    count = count + 1
    #dt = datetime.datetime.now()
    train_id = "T" + time.strftime("%Y%m%d%H%M%S")
    dpu_id = "DPU_01"
    ts = time.time()
    wild_info_msg = {"ts": ts, "train_id" : train_id, "dpu_id": dpu_id, "axle_id": axle_id, "axle_speed": axle_speed, "avg_dyn_load_left": avg_dyn_load_left, "avg_dyn_load_right": avg_dyn_load_right, "max_dyn_load_left": max_dyn_load_left, "max_dyn_load_right" : max_dyn_load_right, "vertical_load_left" : vertical_load_left, "vertical_load_right": vertical_load_right, "lateral_load_left" : lateral_load_left,"lateral_load_right": lateral_load_right, "ilf_left": ilf_left, "ilf_right": ilf_right, "wheel_status_left" : wheel_status_left, "wheel_status_right" : wheel_status_right}
    json_msg = json.dumps(wild_info_msg)
    client.publish("site_01/dpu_01/train_processed_info", json_msg)
    time.sleep(10)
