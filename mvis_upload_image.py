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
import pysftp 
import os

defect_img_list = os.listdir("/data/mvis/yolov5/images")
print(f'{defect_img_list}')

def upload_defect_images(defect_img_list):
    try:
        src_img_path = "/data/mvis/yolov5/images/" + str(folder_name) + "/"
        with pysftp.Connection('157.245.101.18', username='root', password='l2M@11sc') as sftp:
            with sftp.cd('/root/l2m/mvis/src/dashboard_backend/media/images/'):
                for img_name in defect_img_list:
                    print(f'uploading image : {img_name}')
                    sftp.put(src_img_path + img_name)
    except Exception as ex:
        print(f'upload_defect_images: exception: {ex}')

upload_defect_images(defect_img_list)
