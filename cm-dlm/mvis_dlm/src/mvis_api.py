'''
*****************************************************************************
*File : mvis_api.py
*Module : mvis_dlm
*Purpose : mvis data logging module API class for database operations
*Author : Sumankumar Panchal
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python packages'''
import sys
import json
from peewee import *
from datetime import datetime, timedelta

'''Import WILD packages '''
from mvis_model import *
sys.path.insert(1, "../../mvis_common")

from MqttClient import *
from mvis_log import *
from mvis_dlm_conf import *
from mvis_dlm_mqtt import *


class WildAPI:
    '''WILD DAtabase operations such as Select, Insert, Delete records'''
    def __init__(self, cfg, mqtt_client):
        self.dlm_pub = DlmPub()
        self.mqtt_client = mqtt_client
        self.event_msg_id = 0
        self.error_msg_id = 0
        self.psql_db = None
        self.dpu_id = cfg.json_data["DPU_ID"]
        wagon_type_fp = open('wagon_type.json')
        self.wagon_type_dict = json.load(wagon_type_fp)
        wagon_type_fp.close()
        self.latest_train_id = None

    def connect_database(self, config):
        '''Establish connection with database'''
        try:
            self.json_data = config.json_data
            self.db_name = self.json_data["DATABASE"]["DB_NAME"]
            self.user = self.json_data["DATABASE"]["USER"]
            self.password = self.json_data["DATABASE"]["PASSWORD"]
            self.host = self.json_data["DATABASE"]["HOST"]
            self.port = 5432

            if len(self.db_name) == 0:
                Log.logger.critical("mvis_api: connect_database:  database name missing")
                self.msg = "mvis_api: connect_database: database name missing"
                self.error_msg_id += 1
                self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-011", 3, str(self.msg))
            else:
                self.psql_db = PostgresqlDatabase(self.db_name, user = self.user, password = self.password,
                           host = self.host, port = self.port)
                if self.psql_db:
                    try:
                         self.psql_db.connect()
                         Log.logger.info(f'mvis_api: database connection successful')
                         return self.psql_db
                    except Exception as e:
                        Log.logger.critical(f'mvis_api: connect_database: {e}')
                        self.msg = "mvis_api: connect_database:" + str(e)
                        self.error_msg_id += 1
                        self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-012", 3, str(self.msg))
                        sys.exit(1)
                else:
                    return None

        except Exception as e:
            Log.logger.critical("mvis_api: connect_database: Exception: ", e)
            self.msg = "mvis_api: connect_database: Exception: " + str(e)
            self.error_msg_id += 1
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-013", 3, str(self.msg))

    def insert_train_processed_info(self, data):
        '''insert train processed info in database table'''
        try:
            self.train_processed_info = TrainProcessedInfo()
            self.json_data = json.loads(data)
            
            '''Check train_consolidated info is available for the given train_id'''
            self.train_record = self.select_train_consolidated_info(self.json_data["train_id"])

            if len(self.train_record) == 0:
                Log.logger.info(f'Train consolidated info for {self.json_data["train_id"]} - not found')
            else:
                Log.logger.info(f'Train consolidated infor for {self.json_data["train_id"]} - found')
            
                '''list''' 
                self.list_tuple = []
                self.d = []
                for i in range(len(self.json_data["axle_id"])):
                    self.d.append(self.json_data["ts"])
                    self.d.append(self.json_data["train_id"])
                    '''dpu_id insert internally with every record'''
                    self.d.append(self.json_data["dpu_id"])
                    self.d.append(self.json_data["axle_id"][i])
                    self.d.append(self.json_data["axle_speed"][i])
                    self.d.append(self.json_data["avg_dyn_load_left"][i])
                    self.d.append(self.json_data["avg_dyn_load_right"][i])
                    self.d.append(self.json_data["max_dyn_load_left"][i])
                    self.d.append(self.json_data["max_dyn_load_right"][i])
                    self.d.append(self.json_data["vertical_load_left"][i])
                    self.d.append(self.json_data["vertical_load_right"][i])
                    self.d.append(self.json_data["lateral_load_left"][i])
                    self.d.append(self.json_data["lateral_load_right"][i])
                    self.d.append(self.json_data["ilf_left"][i])
                    self.d.append(self.json_data["ilf_right"][i])
                    self.d.append(self.json_data["wheel_status_left"][i])
                    self.d.append(self.json_data["wheel_status_right"][i])
                    self.d.append(self.json_data["v1_fft_max_freq"][i])
                    self.d.append(self.json_data["v1_fft_sd"][i])
                    self.d.append(self.json_data["v2_fft_max_freq"][i])
                    self.d.append(self.json_data["v2_fft_sd"][i])
                    self.d.append(self.json_data["v3_fft_max_freq"][i])
                    self.d.append(self.json_data["v3_fft_sd"][i])
                    self.d.append(self.json_data["v4_fft_max_freq"][i])
                    self.d.append(self.json_data["v4_fft_sd"][i])
                    self.d.append(self.json_data["rake_id"][i])
                    self.t = tuple(self.d)
                    self.list_tuple.append(self.t)
                    self.d.clear()

                Log.logger.info(f'Insert data information: {self.list_tuple}')

                TrainProcessedInfo.insert_many(self.list_tuple, fields=[ \
TrainProcessedInfo.ts, TrainProcessedInfo.train_id, TrainProcessedInfo.dpu_id, TrainProcessedInfo.axle_id, TrainProcessedInfo.axle_speed, \
TrainProcessedInfo.avg_dyn_load_left, TrainProcessedInfo.avg_dyn_load_right, TrainProcessedInfo.max_dyn_load_left, TrainProcessedInfo.max_dyn_load_right,\
TrainProcessedInfo.vertical_load_left, TrainProcessedInfo.vertical_load_right, TrainProcessedInfo.lateral_load_left,\
TrainProcessedInfo.lateral_load_right, TrainProcessedInfo.ilf_left, TrainProcessedInfo.ilf_right,\
TrainProcessedInfo.wheel_status_left, TrainProcessedInfo.wheel_status_right, \
TrainProcessedInfo.v1_fft_max_freq, TrainProcessedInfo.v1_fft_sd, TrainProcessedInfo.v2_fft_max_freq, \
TrainProcessedInfo.v2_fft_sd, TrainProcessedInfo.v3_fft_max_freq, TrainProcessedInfo.v3_fft_sd, \
TrainProcessedInfo.v4_fft_max_freq, TrainProcessedInfo.v4_fft_sd, TrainProcessedInfo.rake_id]).execute()

        except Exception as e:
            Log.logger.critical(f'mvis_api: insert_train_processed_info: Exception raised: {e}')
            self.msg = "mvis_api: insert_train_processed_info: Exception raised: " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-014", 3, str(self.msg))

    def insert_mvis_wagon_info(self, data):
        '''insert train processed info in database table'''
        try:
            json_data = json.loads(data)
            #if json_data["cam_id"] == "left_upper_mono" or json_data["cam_id"] == "right_upper_mono":
            self.mvis_left_wagon_info = MvisLeftWagonInfo()
            wagon_id_list = json_data["wagon_id_list"]
            wagon_type_list = json_data["wagon_type_list"]
                
            Log.logger.info(f'wagon_id_list: {len(wagon_id_list)}, wagon_type_list: {len(wagon_type_list)}')

            new_tagged_wagon_list = []
            new_tagged_bogie_list = []
            actual_wagon_id_list = []
            actual_wagon_type_code_list = []
            actual_wagon_type_list = []
            new_defect_bogie_id_list = []
            new_defect_wagon_id_list = []
            new_defect_code_list = []
            new_defect_image_list = []
            new_side_list = []
            action_taken_list = []
          
            for idx in range(len(json_data["wagon_id_list"])):
                wagon_id = "W" + str(idx+1)
                new_tagged_wagon_list.append(wagon_id)
                new_tagged_bogie_list.append("-")
                new_defect_bogie_id_list.append("-")
                new_defect_code_list.append("-")
                new_defect_image_list.append("-")
                action_taken_list.append("-")
            
            for wagon_type_idx in range(len(wagon_type_list)):
                if str(wagon_type_list[wagon_type_idx]) in self.wagon_type_dict:
                    Log.logger.info(f'wagon_type_idx : {wagon_type_list[wagon_type_idx]}')
                    wagon_type_code = wagon_type_list[wagon_type_idx]
                    wagon_type = self.wagon_type_dict[str(wagon_type_code)]
                    actual_wagon_type_list.append(wagon_type)
                else:
                    wagon_type = "-"
                    actual_wagon_type_list.append(wagon_type)
                
            list_tuple = []
            d = []
            for idx in range(len(json_data["wagon_id_list"])):
                d.append(json_data["ts"])
                #d.append(json_data["train_id"])
                d.append(self.latest_train_id)
                '''dpu_id insert internally with every record'''
                d.append(json_data["dpu_id"])
                d.append(wagon_id_list[idx])
                d.append(actual_wagon_type_list[idx])
                d.append(new_tagged_wagon_list[idx])
                d.append(new_tagged_bogie_list[idx])
                d.append(new_defect_code_list[idx])
                d.append(new_defect_image_list[idx])
                #if json_data["cam_id"] == "left_upper_mono":
                d.append("Left")
                #if json_data["cam_id"] == "right_upper_mono":
                #d.append("Right")
                d.append(action_taken_list[idx])

                t = tuple(d)
                list_tuple.append(t)
                d.clear()

            Log.logger.info(f'Insert left wagon information: {list_tuple}')

            #if json_data["cam_id"] == "left_upper_mono":
            MvisLeftWagonInfo.insert_many(list_tuple, fields=[ \
                        MvisLeftWagonInfo.ts, MvisLeftWagonInfo.train_id, MvisLeftWagonInfo.dpu_id, \
                        MvisLeftWagonInfo.wagon_id, MvisLeftWagonInfo.wagon_type, \
                        MvisLeftWagonInfo.tagged_wagon_id, MvisLeftWagonInfo.tagged_bogie_id, \
                        MvisLeftWagonInfo.defect_code, MvisLeftWagonInfo.defect_image, MvisLeftWagonInfo.side,\
                        MvisLeftWagonInfo.action_taken]).execute()
            #else:
                #pass

            #if json_data["cam_id"] == "right_upper_mono":
            MvisRightWagonInfo.insert_many(list_tuple, fields=[ \
                        MvisRightWagonInfo.ts, MvisRightWagonInfo.train_id, MvisRightWagonInfo.dpu_id, \
                        MvisRightWagonInfo.wagon_id, MvisRightWagonInfo.wagon_type, \
                        MvisRightWagonInfo.tagged_wagon_id, MvisRightWagonInfo.tagged_bogie_id, \
                        MvisRightWagonInfo.defect_code, MvisRightWagonInfo.defect_image, MvisRightWagonInfo.side,\
                        MvisRightWagonInfo.action_taken]).execute()
                #else:
                    #pass
            #else:
                #pass
        except Exception as ex:
            Log.logger.critical(f'insert_wagon_info: exception: {ex}')

    def insert_mvis_processed_info(self, data):
        '''insert train processed info in database table'''
        try:
            self.mvis_processed_info = MvisProcessedInfo()
            self.json_data = json.loads(data)
            
            new_tagged_wagon_list = []
            new_tagged_bogie_list = []
            actual_wagon_id_list = []
            actual_wagon_type_code_list = []
            actual_wagon_type_list = []
            new_defect_bogie_id_list = []
            new_defect_wagon_id_list = []
            new_defect_code_list = []
            new_defect_image_list = []
            new_defect_side_list = []
            action_taken_list = []

            if len(self.json_data["defects"]) == 0:
                Log.logger.info(f'no defects found!!')
            
            else:

                for idx in range(len(self.json_data["defects"])):
                    if "defect_bogie_id" in self.json_data["defects"][idx]:
                        #bogie_id = "B" + str(idx+1)
                        bogie_id = self.json_data["defects"][idx]["defect_bogie_id"]
                        new_tagged_bogie_list.append(bogie_id)
                        new_defect_bogie_id_list.append("-")
                        new_defect_code_list.append("-")
                        new_defect_image_list.append("-")
                        new_defect_side_list.append("-")
                        action_taken_list.append("-")
                    
                    else:
                        Log.logger.info(f'defect bogie id not found!!')

                
                for idx in range(len(self.json_data["defects"])):
                    if "defect_wagon_id" in self.json_data["defects"][idx]:
                        wagon_id = self.json_data['defects'][idx]['defect_wagon_id']
                        new_tagged_wagon_list.append(wagon_id)
                        new_defect_wagon_id_list.append("none")
                        new_defect_wagon_id_list.append("none")

                    else:
                        Log.logger.info(f'defect wagon id not found!!')

                for defect_idx in range(len(self.json_data["defects"])):
                    if "defect_bogie_id" in self.json_data["defects"][defect_idx]:
                        if self.json_data["defects"][defect_idx]["defect_bogie_id"] == new_tagged_bogie_list[defect_idx]:
                            new_defect_bogie_id_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_bogie_id"]
                            new_defect_code_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_code"]
                            new_defect_image_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_image"]
                            new_defect_side_list[defect_idx] = self.json_data["defects"][defect_idx]["side"]
                        else:
                            pass
                    else:
                        Log.logger.info(f'defect bogie id not found!!')
            
                for defect_idx in range(len(self.json_data["defects"])):
                    if "defect_wagon_id" in self.json_data["defects"][defect_idx]:
                        if self.json_data["defects"][defect_idx]["defect_wagon_id"] == new_tagged_wagon_list[defect_idx]:
                            #if self.json_data["cam_id"] == "left_upper_mono" or self.json_data["cam_id"] == "right_upper_mono":
                            new_defect_wagon_id_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_wagon_id"]
                            new_defect_code_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_code"]
                            new_defect_image_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_image"]
                            new_defect_side_list[defect_idx] = self.json_data["defects"][defect_idx]["side"]
                            #else:
                                #pass
                                
                            if new_defect_bogie_id_list[defect_idx] != '-':
                                new_defect_wagon_id_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_wagon_id"]
                                new_defect_code_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_code"]
                                new_defect_image_list[defect_idx] = self.json_data["defects"][defect_idx]["defect_image"]
                                new_defect_side_list[defect_idx] = self.json_data["defects"][defect_idx]["side"]
                            else:
                                pass
                        else:
                            pass
                    else:
                        Log.logger.info(f'defect wagon id not found')

            Log.logger.info(f'New tagged wagon list: {new_tagged_wagon_list}')
            Log.logger.info(f'New tagged bogie list: {new_tagged_bogie_list}')
            Log.logger.info(f'New defect bogie id list: {new_defect_bogie_id_list}')
            Log.logger.info(f'New defect code list: {new_defect_code_list}')
            Log.logger.info(f'New defect image list: {new_defect_image_list}')
            Log.logger.info(f'New defect side list: {new_defect_side_list}')

            '''list''' 
            self.list_tuple = []
            self.d = []
            
            for idx in range(len(self.json_data["defects"])):
                if "defect_bogie_id" in self.json_data["defects"][idx]:
                    self.d.append(self.json_data["ts"])
                    #self.d.append(self.json_data["train_id"])
                    self.d.append(self.latest_train_id)
                    '''dpu_id insert internally with every record'''
                    self.d.append(self.json_data["dpu_id"])
                    #self.d.append(actual_wagon_id_list[idx])
                    #self.d.append(actual_wagon_type_list[idx])
                    self.d.append("-")
                    self.d.append("-")
                    self.d.append(new_tagged_wagon_list[idx])
                    self.d.append(new_tagged_bogie_list[idx])
                    self.d.append(new_defect_code_list[idx])
                    self.d.append(new_defect_image_list[idx])
                    self.d.append(new_defect_side_list[idx])
                    self.d.append(action_taken_list[idx])
                    
                    self.t = tuple(self.d)
                    self.list_tuple.append(self.t)
                    self.d.clear()

                else:
                    Log.logger.info(f'defect bogie id not found!!')

            Log.logger.info(f'Insert data information: {self.list_tuple}')

            MvisProcessedInfo.insert_many(self.list_tuple, fields=[ \
                    MvisProcessedInfo.ts, MvisProcessedInfo.train_id, MvisProcessedInfo.dpu_id, \
                    MvisProcessedInfo.wagon_id, MvisProcessedInfo.wagon_type, \
                    MvisProcessedInfo.tagged_wagon_id, MvisProcessedInfo.tagged_bogie_id, \
                    MvisProcessedInfo.defect_code, MvisProcessedInfo.defect_image, MvisProcessedInfo.side,\
                    MvisProcessedInfo.action_taken]).execute()

        except Exception as e:
            Log.logger.critical(f'mvis_api: insert_mvis_processed_info: Exception raised: {e}')
            self.msg = "mvis_api: insert_mvis_processed_info: Exception raised: " + str(e)
            
    def train_processed_info_mem_mgmt(self, train_id):
        '''Perform memory management of train_processed_info table'''
        try:
            self.train_id = train_id
            self.query = TrainProcessedInfo.delete().where(TrainProcessedInfo.train_id == train_id)
            self.query.execute()
            Log.logger.info(f'Deleted {self.train_id} from train_processed_info table')

        except Exception as e:
            Log.logger.critical(f'mvis_api: train_processed_info_mem_mgmt: exception: {e}') 
            self.msg = "mvis_api: train_processed_info_mem_mgmt: exception: " + str(e)
            self.error_msg_id += 1
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-015", 3, str(self.msg))

    def test_insert_train_processed_info(self):
        '''test insert operation with sample data'''
        try:
            self.tp_info_table = TrainProcessedInfo()
            self.tp_info_table.ts = time.time()
            self.tp_info_table.train_id = "T" + time.strftime("%Y%m%d%H%M%S") 
            self.tp_info_table.dpu_id = "DPU_01"
            self.tp_info_table.axle_id = 1 
            self.tp_info_table.axle_speed = 80
            self.tp_info_table.avg_dyn_load_left = 5.5
            self.tp_info_table.avg_dyn_load_right = 5.5 
            self.tp_info_table.max_dyn_load_left = 10.5 
            self.tp_info_table.max_dyn_load_right = 10.5 
            self.tp_info_table.vertical_load_left = 6.5 
            self.tp_info_table.vertical_load_right = 6.5 
            self.tp_info_table.lateral_load_left = 5.0 
            self.tp_info_table.lateral_load_right = 5.0 
            self.tp_info_table.ilf_left = 1.9
            self.tp_info_table.ilf_right = 1.9 
            self.tp_info_table.wheel_status_left = 1
            self.tp_info_table.wheel_status_right = 1
            self.tp_info_table.v1_fft_max_freq = 1
            self.tp_info_table.v1_fft_sd = 1.1
            self.tp_info_table.v2_fft_max_freq = 1
            self.tp_info_table.v2_fft_sd = 1.1
            self.tp_info_table.v3_fft_max_freq = 1
            self.tp_info_table.v3_fft_sd = 1.1
            self.tp_info_table.v4_fft_max_freq = 1
            self.tp_info_table.v4_fft_sd = 1.1
            self.tp_info_table.save()
            Log.logger.info(f'mvis_api: test_insert_train_processed_info: record inserted: {self.tp_info_table.train_id}')
        except Exception as e:
            Log.logger.critical("mvis_api: test_insert_train_processed_info : Exception generated", e)
            self.msg = "mvis_api: test_insert_train_processed_info : exception: " + str(e)
            self.error_msg_id += 1
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-016", 3, str(self.msg))
    
    def select_train_processed_info(self, train_id):
        '''Get store records from train_processed_info'''
        try:
            self.train_id = train_id
            self.model_name = TrainProcessedInfo() 
            self.records = self.model_name.select().where(train_id == self.train_id)
            return self.records
        except Exception as e:
            Log.logger.critical("mvis_api: select_train_processed_info : exception :", e)
            self.msg = "mvis_api: select_train_processed_info : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-017", 3, str(self.msg))
    
    def insert_train_consolidated_info(self, data):
        '''insert train consolidated info in train_consolidated_info table'''
        try:
            self.json_data = json.loads(data)
            
            self.train_table = TrainConsolidatedInfo()
            self.train_table.train_id = self.json_data["train_id"]
            self.train_table.dpu_id = self.json_data["dpu_id"]
            self.train_table.entry_time = self.json_data["train_entry_time"]
            self.train_table.exit_time = self.json_data["train_exit_time"]
            self.train_table.total_axles = self.json_data["total_axles"]
            self.train_table.total_wheels = self.json_data["total_wheels"]
            self.train_table.total_bad_wheels = self.json_data["total_bad_wheels"]
            self.train_table.direction = self.json_data["direction"]
            self.train_table.train_speed = self.json_data["train_speed"]
            self.train_table.ilf_threshold_warning = self.json_data["ilf_threshold_warning"]
            self.train_table.ilf_threshold_critical = self.json_data["ilf_threshold_critical"]
            self.train_table.mdil_threshold_warning = self.json_data["mdil_threshold_warning"]
            self.train_table.mdil_threshold_critical = self.json_data["mdil_threshold_critical"]
            self.train_table.train_type = self.json_data["train_type"]
            self.train_table.train_processed = self.json_data["train_processed"]
            self.train_table.remark = self.json_data["remark"]

            self.train_table.save(force_insert=True)
            ''' perform memory management '''
            #self.train_consolidated_info_mem_mgmt()
            Log.logger.info(f'mvis_api: insert_train_consolidated_info: record inserted')
            
            ''' train id for mvis defects'''
            self.latest_train_id = self.json_data["train_id"]

        except Exception as e:
            Log.logger.critical(f'mvis_api: insert_train_consolidated_info: exception {e}')
            self.msg = "mvis_api: insert_train_consolidated_info : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-018", 3, str(self.msg))

    def train_consolidated_info_mem_mgmt(self):
        '''Perform memory management of train_consolidated_info table'''
        try:
            self.records = (TrainConsolidatedInfo.select(TrainConsolidatedInfo.train_id).order_by(TrainConsolidatedInfo.train_id))
            self.total_records = len(self.records)
            Log.logger.info(f'No.of records in train_consolidated_info table: {self.total_records}')
            
            if(self.total_records > 10000):
                Log.logger.info(self.records[0].train_id)
                self.query = TrainConsolidatedInfo.delete().where(TrainConsolidatedInfo.train_id == self.records[0].train_id)
                self.query.execute()
                Log.logger.info(f'Deleted first record in train_consolidated_info table')
                
                '''train_processed_info memory management'''
                self.train_processed_info_mem_mgmt(self.records[0].train_id)

        except Exception as e:
            Log.logger.critical(f'mvis_api: train_consolidated_info_mem_mgmt : exception: {e}') 
            self.msg = "mvis_api: train_consolidated_info_mem_mgmt : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-019", 3, str(self.msg))

    def select_train_consolidated_info(self, train_id):
        '''Get train_consolidated_info records'''
        try:
            Log.logger.info(f'select_train_consolidated_info of {train_id}')
            self.train_id = train_id
            self.records = TrainConsolidatedInfo.select().where(TrainConsolidatedInfo.train_id == self.train_id).order_by(TrainConsolidatedInfo.train_id)
            return self.records
        except Exception as e:
            Log.logger.critical("mvis_api: select_train_consolidated_info: exception :", e)
            self.msg = "mvis_api: select_train_consolidated_info : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-020", 3, str(self.msg))
    
    def insert_mvis_error_info(self, data):
        ''' Insert error info in table '''
        try:
            self.json_data = json.loads(data)
            
            self.error_info = ErrorInfo()
            self.error_info.ts = self.json_data["ts"]
            self.error_info.msg_id = self.json_data["msg_id"]
            self.error_info.dpu_id = self.json_data["dpu_id"]
            self.error_info.error_id = self.json_data["error_id"]
            self.error_info.error_severity = self.json_data["error_severity"]
            self.error_info.error_desc = self.json_data["error_desc"]
            self.error_info.save()
            
            '''perform memory management'''
            #self.error_info_mem_mgmt()
            Log.logger.info(f'mvis_api: insert_mvis_error_info: record inserted')
        except Exception as e:
            Log.logger.critical(f'mvis_api: insert_mvis_error_info: exception {e}')
            self.msg = "mvis_api: insert_mvis_error_info : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-021", 3, str(self.msg))

    def insert_mvis_event_info(self, data):
        ''' Insert event info in table '''
        try:
            self.json_data = json.loads(data)
            
            self.event_info = EventInfo()
            self.event_info.ts = self.json_data["ts"]
            self.event_info.msg_id = self.json_data["msg_id"]
            self.event_info.dpu_id = self.json_data["dpu_id"]
            self.event_info.event_id = self.json_data["event_id"]
            self.event_info.event_desc = self.json_data["event_desc"]
            self.event_info.save()
            
            '''perform memory management''' 
            #self.event_info_mem_mgmt()
            Log.logger.info(f'mvis_api: insert_mvis_event_info: record inserted')
        except Exception as e:
            Log.logger.critical(f'mvis_api: insert_mvis_event_info: exception : {e}')
            self.msg = "mvis_api: insert_mvis_event_info : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-022", 3, str(self.msg))

    def event_info_mem_mgmt(self):
        '''keep 6 months events data'''
        try:
            self.event_query = (EventInfo.delete().where(fn.to_timestamp(EventInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            self.event_query.execute()
        except Exception as e:
            Log.logger.critical(f'mvis_api: event_info_mem_mgmt : exception : {e}') 
            self.msg = "mvis_api: event_info_mem_mgmt : exception : " + str(e)
            self.error_msg_id += 1
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-023", 3, str(self.msg))

    def error_info_mem_mgmt(self):
        '''keep 6 months data'''
        try:
            self.error_query = (ErrorInfo.delete().where(fn.to_timestamp(ErrorInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            self.error_query.execute()
        except Exception as e:
            Log.logger.critical(f'mvis_api: error_info_mem_mgmt: exception : {e}')
            self.msg = "mvis_api: error_info_mem_mgmt : exception : " + str(e)
            self.error_msg_id += 1
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-024", 3, str(self.msg))

    def insert_mvis_health_info(self, data):
        ''' Insert health info in table '''
        try:
            self.json_data = json.loads(data)

            self.health_info = HealthInfo()
            self.health_info.ts = self.json_data["ts"]
            self.health_info.dpu_id = self.json_data["dpu_id"]
            self.health_info.comm_link = self.json_data["comm_link"]
            self.health_info.interrogator_link = self.json_data["interrogator_link"]
            self.health_info.s1_link = self.json_data["S1"]
            self.health_info.s2_link = self.json_data["S2"] 
            self.health_info.s3_link = self.json_data["S3"]
            self.health_info.s4_link = self.json_data["S4"]
            self.health_info.s5_link = self.json_data["S5"]
            self.health_info.s6_link = self.json_data["S6"]
            self.health_info.s7_link = self.json_data["S7"]
            self.health_info.s8_link = self.json_data["S8"]
            self.health_info.s9_link = self.json_data["S9"]
            self.health_info.s10_link = self.json_data["S10"]
            self.health_info.s11_link = self.json_data["S11"]
            self.health_info.s12_link = self.json_data["S12"]
            self.health_info.v1_link = self.json_data["V1"]
            self.health_info.v2_link = self.json_data["V2"]
            self.health_info.v3_link = self.json_data["V3"]
            self.health_info.v4_link = self.json_data["V4"]
            self.health_info.l1_link = self.json_data["L1"]
            self.health_info.l2_link = self.json_data["L2"]
            self.health_info.l3_link = self.json_data["L3"]
            self.health_info.l4_link = self.json_data["L4"]
            self.health_info.t1_link = self.json_data["T1"]
            self.health_info.t2_link = self.json_data["T2"]
            self.health_info.t3_link = self.json_data["T3"]
            self.health_info.t4_link = self.json_data["T4"]
            self.health_info.save()

            '''perform memory management'''
            #self.health_info_mem_mgmt()
            Log.logger.info(f'mvis_api: insert_mvis_health_info: record inserted')
        except Exception as e:
            Log.logger.critical(f'mvis_api: insert_mvis_health_info: exception : {e}')
            self.msg = "mvis_api: insert_mvis_health_info : exception : " + str(e)
            self.error_msg_id += 1
            self.psql_db.rollback()
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-025", 3, str(self.msg))

    def health_info_mem_mgmt(self):
        '''keep 6 months data'''
        try:
            self.health_query = (HealthInfo.delete().where(fn.to_timestamp(HealthInfo.ts) < datetime.now() + timedelta(days=-180, hours=0)))
            self.health_query.execute()
        except Exception as e:
            Log.logger.critical(f'mvis_api: health_info_mem_mgmt: exception : {e}')
            self.msg = "mvis_api: health_info_mem_mgmt : exception : " + str(e)
            self.error_msg_id += 1
            #self.dlm_pub.publish_error_info(self.mqtt_client, self.dpu_id, self.error_msg_id, "DLM-ERROR-026", 3, str(self.msg))

if __name__ == '__main__':
    if Log.logger is None:
      my_log = Log()

    cfg = WildDlmConfRead()
    cfg.read_cfg('../../config/mvis_dlm.conf')

    mqtt_client = MqttClient("127.0.0.1", 1883, "dlm-api", '', '', 'dlm-api')
    mqtt_client.connect()

    mvis_api = WildAPI(cfg, mqtt_client)

    db_conn = mvis_api.connect_database(cfg)
    
    if db_conn:
        #mvis_api.test_insert_train_processed_info()
        mvis_api.train_consolidated_info_mem_mgmt()
        mvis_api.event_info_mem_mgmt()
        mvis_api.error_info_mem_mgmt()
    else:
        pass
