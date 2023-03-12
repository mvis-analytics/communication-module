# from twilio.rest import Client
import time
import json
from datetime import datetime

from MqttClient import MqttClient


msg_train_consolidated_info = {"dpu_id": "DPU_01",
                               "train_id": "T20210912032753",
                               "train_entry_time": 1631397470.453871,
                               "train_exit_time": 1631397501.051174,
                               "total_axles": 70, "total_wheels": 140,
                               "total_bad_wheels": 2,
                               "direction": "DBU->YNK",
                               "train_speed": 77.51,
                               "ilf_threshold_warning": 2.0,
                               "ilf_threshold_critical": 4.5
                               }

msg_train_processed_info =  {'train_id': 'T20220118035341', 'dpu_id': 'DPU_01', 'ts': 1642458221.0, 'axle_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70], 'axle_speed': [94.44, 94.56, 94.69, 94.43, 94.75, 94.44, 94.52, 94.35, 94.04, 94.09, 94.19, 93.78, 93.93, 93.84, 93.54, 94.04, 93.78, 93.85, 93.85, 93.48, 94.03, 93.68, 93.49, 93.26, 92.91, 93.3, 93.13, 93.03, 92.9, 92.77, 93.2, 92.12, 92.5, 93.84, 92.45, 92.45, 91.99, 91.88, 93.0, 91.55, 91.55, 91.57, 91.57, 91.12, 90.87, 90.95, 90.35, 90.33, 89.99, 90.3, 92.18, 89.6, 88.78, 89.31, 88.74, 88.53, 87.8, 87.86, 87.86, 87.2, 86.57, 86.51, 85.99, 86.42, 85.43, 85.25, 84.57, 84.24, 83.4, 83.38], 'avg_dyn_load_left': [10.94, 9.74, 11.09, 10.97, 10.16, 10.19, 7.67, 7.21, 7.39, 7.59, 7.58, 7.18, 7.7, 7.46, 7.33, 7.15, 7.75, 7.54, 7.44, 7.16, 7.29, 7.06, 7.91, 7.43, 7.23, 7.08, 7.65, 6.9, 7.58, 6.98, 9.02, 7.96, 7.29, 7.08, 7.5, 7.33, 7.62, 7.29, 8.24, 6.67, 7.47, 7.25, 8.21, 8.14, 7.52, 7.34, 6.41, 6.55, 8.1, 7.65, 10.56, 8.68, 6.83, 7.06, 6.62, 6.88, 7.17, 6.56, 7.59, 7.72, 7.28, 7.13, 7.47, 7.0, 7.65, 7.47, 9.19, 8.86, 8.92, 8.75], 'avg_dyn_load_right': [9.3, 8.82, 9.98, 9.76, 8.66, 10.18, 6.09, 6.05, 6.75, 6.89, 7.37, 7.17, 7.15, 6.88, 7.22, 7.15, 6.79, 6.35, 6.77, 6.48, 6.47, 6.42, 6.34, 6.46, 7.17, 6.99, 6.79, 7.23, 6.95, 7.33, 7.43, 7.83, 8.02, 8.86, 6.67, 6.44, 6.4, 6.46, 7.77, 7.81, 6.8, 6.6, 6.29, 5.95, 6.65, 6.45, 7.69, 7.27, 6.27, 6.11, 8.21, 7.06, 8.09, 7.07, 7.0, 6.11, 6.83, 6.72, 6.83, 6.51, 7.42, 6.66, 6.87, 6.82, 6.99, 6.53, 8.38, 7.69, 7.36, 7.04], 'max_dyn_load_left': [15.94, 17.48, 18.0, 15.45, 16.72, 15.73, 11.34, 11.48, 11.34, 12.36, 10.17, 11.57, 11.12, 12.14, 10.31, 11.46, 10.87, 11.32, 10.55, 11.6, 10.18, 11.09, 10.7, 12.25, 10.13, 11.07, 9.7, 11.38, 10.38, 11.15, 20.1, 23.66, 9.92, 12.24, 10.31, 12.02, 10.27, 11.05, 21.97, 10.68, 17.01, 12.02, 10.4, 13.06, 10.37, 12.56, 9.35, 10.51, 12.44, 11.84, 23.91, 12.67, 16.83, 14.1, 10.05, 11.4, 9.93, 10.19, 10.18, 12.19, 10.12, 11.81, 12.7, 11.74, 9.92, 11.44, 13.38, 13.39, 12.52, 13.42], 'max_dyn_load_right': [12.09, 12.31, 13.24, 12.28, 12.1, 12.39, 9.01, 8.12, 10.07, 10.43, 10.61, 10.59, 9.39, 8.99, 10.28, 10.44, 9.26, 8.73, 8.66, 8.44, 8.45, 8.18, 8.51, 8.54, 9.04, 9.51, 9.72, 9.68, 10.36, 9.93, 33.5, 15.97, 11.48, 23.1, 9.8, 8.82, 8.61, 7.98, 24.13, 11.53, 8.8, 9.02, 8.22, 8.07, 9.08, 12.87, 10.68, 11.57, 9.01, 9.43, 19.37, 10.97, 28.83, 16.51, 9.69, 7.77, 8.38, 8.93, 9.45, 12.74, 10.22, 9.55, 8.76, 10.97, 10.4, 10.43, 11.93, 9.98, 11.03, 13.99], 'vertical_load_left': [15.94, 17.48, 18.0, 15.45, 16.72, 15.73, 11.34, 11.48, 11.34, 12.36, 10.17, 11.57, 11.12, 12.14, 10.31, 11.46, 10.87, 11.32, 10.55, 11.6, 10.18, 11.09, 10.7, 12.25, 10.13, 11.07, 9.7, 11.38, 10.38, 11.15, 20.1, 23.66, 9.92, 12.24, 10.31, 12.02, 10.27, 11.05, 21.97, 10.68, 17.01, 12.02, 10.4, 13.06, 10.37, 12.56, 9.35, 10.51, 12.44, 11.84, 23.91, 12.67, 16.83, 14.1, 10.05, 11.4, 9.93, 10.19, 10.18, 12.19, 10.12, 11.81, 12.7, 11.74, 9.92, 11.44, 13.38, 13.39, 12.52, 13.42], 'vertical_load_right': [12.09, 12.31, 13.24, 12.28, 12.1, 12.39, 9.01, 8.12, 10.07, 10.43, 10.61, 10.59, 9.39, 8.99, 10.28, 10.44, 9.26, 8.73, 8.66, 8.44, 8.45, 8.18, 8.51, 8.54, 9.04, 9.51, 9.72, 9.68, 10.36, 9.93, 33.5, 15.97, 11.48, 23.1, 9.8, 8.82, 8.61, 7.98, 24.13, 11.53, 8.8, 9.02, 8.22, 8.07, 9.08, 12.87, 10.68, 11.57, 9.01, 9.43, 19.37, 10.97, 28.83, 16.51, 9.69, 7.77, 8.38, 8.93, 9.45, 12.74, 10.22, 9.55, 8.76, 10.97, 10.4, 10.43, 11.93, 9.98, 11.03, 13.99], 'lateral_load_left': [1.79, 0.75, 1.42, 0.71, 0.81, 0.71, 1.18, 0.28, 0.49, 1.12, 1.32, 0.8, 0.35, 1.14, 1.05, 0.88, 0.76, 1.01, 1.4, 0.94, 1.01, 0.84, 0.52, 0.61, 1.24, 0.51, 0.85, 0.4, 0.63, 0.42, 0.97, 0.98, 0.91, 0.71, 0.78, 0.6, 0.77, 0.49, 2.23, 0.24, 0.68, 0.28, 1.38, 0.48, 0.54, 0.36, 0.57, 0.5, 1.25, 0.27, 2.28, 0.26, 1.01, 2.58, 0.98, 0.68, 0.85, 0.81, 1.1, 0.5, 1.05, 0.2, 1.08, 1.01, 0.77, 1.12, 1.99, 1.32, 1.67, 0.34], 'lateral_load_right': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 'ilf_left': [1.46, 1.8, 1.62, 1.41, 1.65, 1.54, 1.48, 1.59, 1.53, 1.63, 1.34, 1.61, 1.44, 1.63, 1.41, 1.6, 1.4, 1.5, 1.42, 1.62, 1.4, 1.57, 1.35, 1.65, 1.4, 1.56, 1.27, 1.65, 1.37, 1.6, 2.23, 2.97, 1.36, 1.73, 1.38, 1.64, 1.35, 1.52, 2.67, 1.6, 2.28, 1.66, 1.27, 1.6, 1.38, 1.71, 1.46, 1.6, 1.54, 1.55, 2.26, 1.46, 2.47, 2.0, 1.52, 1.66, 1.39, 1.55, 1.34, 1.58, 1.39, 1.66, 1.7, 1.68, 1.3, 1.53, 1.46, 1.51, 1.4, 1.53], 'ilf_right': [1.3, 1.4, 1.33, 1.26, 1.4, 1.22, 1.48, 1.34, 1.49, 1.51, 1.44, 1.48, 1.31, 1.31, 1.42, 1.46, 1.36, 1.38, 1.28, 1.3, 1.31, 1.27, 1.34, 1.32, 1.26, 1.36, 1.43, 1.34, 1.49, 1.35, 4.51, 2.04, 1.43, 2.61, 1.47, 1.37, 1.34, 1.24, 3.11, 1.48, 1.29, 1.37, 1.31, 1.36, 1.37, 1.99, 1.39, 1.59, 1.44, 1.54, 2.36, 1.55, 3.57, 2.33, 1.38, 1.27, 1.23, 1.33, 1.38, 1.96, 1.38, 1.43, 1.28, 1.61, 1.49, 1.6, 1.42, 1.3, 1.5, 1.99], 'wheel_status_left': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 1, 2, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], 'wheel_status_right': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 2, 1, 2, 1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 1, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], 'v1_fft_max_freq': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v1_fft_sd': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v2_fft_max_freq': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v2_fft_sd': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v3_fft_max_freq': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v3_fft_sd': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v4_fft_max_freq': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], 'v4_fft_sd': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]}


if __name__ == "__main__":

    # from subprocess import call
    #
    # call(["powershell", "-command", "(gcim Win32_OperatingSystem).LastBootUpTime"])

    print(f'START OF PM SIMULATOR : {datetime.now()}')
    tpi_topic = "site_01/dpu_01/train_processed_info"
    # tci_topic = "site_01/dpu_01/train_consolidated_info"
    # tpi_topic = "dpu_pm/train_processed_info"
    # tci_topic = "dpu_pm/train_consolidated_info"

    # Create an object for MQTT Client and connect to the broker
    # mqtt_clobj = MqttClient("127.0.0.1", 1883, 'CM_Simulator', '', '', 'CM_Simulator')
    mqtt_clobj = MqttClient("203.153.42.143", 1883, 'CM_Simulator', '', '', 'CM_Simulator')
    mqtt_clobj.connect()

    # print(f'Sending      = {tci_topic}')
    # print(f'--------------------------------------------------')
    # json_msg = json.dumps(msg_train_consolidated_info)
    # print(json_msg)

    # mqtt_clobj.pub(tci_topic, json_msg)
    # time.sleep(5)

    print(f'Sending      = {tpi_topic}')
    print(f'--------------------------------------------------')
    json_msg = json.dumps(msg_train_processed_info)
    print(json_msg)

    mqtt_clobj.pub(tpi_topic, json_msg)
    time.sleep(5)

    print(f'END  OF CM SIMULATOR : {datetime.now()}')
