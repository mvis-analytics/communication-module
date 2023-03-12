'''
*****************************************************************************
*File : mvis_model.py
*Module : mvis_dlm 
*Purpose : Database model class for design postgresql database to store data. 
*Author : Sumankumar Panchal 
*Copyright : Copyright 2020, Lab to Market Innovations Private Limited
*****************************************************************************
'''

'''Import python module'''
from peewee import *
from datetime import datetime
import sys

'''Import mvis module'''
sys.path.insert(1,"../../mvis_common")
from mvis_log import *
from mvis_dlm_conf import *

if Log.logger is None:
    my_log = Log()

'''read configuration file'''
cfg = WildDlmConfRead()
cfg.read_cfg('../../config/mvis_dlm.conf')

json_data = cfg.json_data
db_name = json_data["DATABASE"]["DB_NAME"]
user = json_data["DATABASE"]["USER"]
password = json_data["DATABASE"]["PASSWORD"]
host = json_data["DATABASE"]["HOST"]
port = 5432
psql_db = None

try:  
    psql_db = PostgresqlDatabase(db_name, user= user, password= password, host=host, port=port)
    if psql_db != None:
        psql_db.connect()
except Exception as e:
    Log.logger.critical(f'mvis_dlm_model: Exception: {e}')

class WildModel(Model):
    """A base model that will use our Postgresql database"""
    class Meta:
        database = psql_db

class MvisProcessedInfo(WildModel):
    '''MVIS processed information table'''
    ts = DoubleField()
    train_id = CharField()
    dpu_id = CharField()
    wagon_id = CharField(null = True)
    wagon_type = CharField(null = True)
    tagged_wagon_id = CharField(null = True)
    tagged_bogie_id = CharField(null = True)
    defect_code = CharField(null = True)
    defect_image = CharField(null = True)
    side = CharField(null = True)
    action_taken = CharField(null = True)
    
    class Meta:
        table_name = "mvis_processed_info"

class TrainProcessedInfo(WildModel):
    ''' Train processed information table '''
    ts = DoubleField()
    train_id = CharField()
    dpu_id = CharField()
    axle_id = IntegerField()
    axle_speed = FloatField(null = True)
    avg_dyn_load_left = FloatField(null = True)
    avg_dyn_load_right = FloatField(null = True)
    max_dyn_load_left = FloatField(null = True)
    max_dyn_load_right = FloatField(null = True)
    vertical_load_left = FloatField(null = True)
    vertical_load_right = FloatField(null = True)
    lateral_load_left = FloatField(null = True)
    lateral_load_right = FloatField(null = True)
    ilf_left = FloatField(null = True)
    ilf_right = FloatField(null = True)
    wheel_status_left = SmallIntegerField(null = True)
    wheel_status_right = SmallIntegerField(null = True)
    v1_fft_max_freq = IntegerField(null = True)
    v1_fft_sd = DoubleField(null = True)
    v2_fft_max_freq = IntegerField(null = True)
    v2_fft_sd = DoubleField(null = True)
    v3_fft_max_freq = IntegerField(null = True)
    v3_fft_sd = DoubleField(null = True)
    v4_fft_max_freq = IntegerField(null = True)
    v4_fft_sd = DoubleField(null = True)
    rake_id = CharField()

    class Meta:
        table_name = "train_processed_info"

class TrainConsolidatedInfo(WildModel):
    ''' Train consolidated information table '''
    train_id = CharField()
    dpu_id = CharField()
    entry_time = DoubleField(null = True)
    exit_time = DoubleField(null = True)
    total_axles = SmallIntegerField(null = True)
    total_wheels = SmallIntegerField(null = True)
    total_bad_wheels = SmallIntegerField(null = True)
    direction = CharField(null = True)
    train_speed = FloatField(null = True)
    ilf_threshold_warning = FloatField (null = True)
    ilf_threshold_critical = FloatField (null = True)
    mdil_threshold_warning = DoubleField (null = True)
    mdil_threshold_critical = DoubleField (null = True)
    train_type = CharField()
    train_processed = BooleanField()
    remark = CharField()

    class Meta:
        table_name = "train_consolidated_info"

class EventInfo(WildModel):
    ''' Event information table '''
    ts = DoubleField()
    dpu_id = CharField()
    msg_id = IntegerField(null = True)
    event_id = CharField(null = True)
    event_desc = TextField(null = True)
    
    class Meta:
        table_name = "event_info"

class ErrorInfo(WildModel):
    ''' Error information table '''
    ts = DoubleField()
    dpu_id = CharField()
    msg_id = IntegerField(null = True)
    error_id = CharField(null = True)
    error_severity = IntegerField(null = True)
    error_desc = TextField(null = True)

    class Meta:
        table_name = "error_info"

class HealthInfo(WildModel):
    #Health information table
    ts = DoubleField()
    dpu_id = CharField()
    comm_link = CharField(null = True)
    interrogator_link = CharField(null = True)
    s1_link = CharField(null = True)
    s2_link = CharField(null = True)
    s3_link = CharField(null = True)
    s4_link = CharField(null = True)
    s5_link = CharField(null = True)
    s6_link = CharField(null = True)
    s7_link = CharField(null = True)
    s8_link = CharField(null = True)
    s9_link = CharField(null = True)
    s10_link = CharField(null = True)
    s11_link = CharField(null = True)
    s12_link = CharField(null = True)
    v1_link = CharField(null = True)
    v2_link = CharField(null = True)
    v3_link = CharField(null = True)
    v4_link = CharField(null = True)
    l1_link = CharField(null = True)
    l2_link = CharField(null = True)
    l3_link = CharField(null = True)
    l4_link = CharField(null = True)
    t1_link = CharField(null = True)
    t2_link = CharField(null = True)
    t3_link = CharField(null = True)
    t4_link = CharField(null = True)

    class Meta:
        table_name = "health_info"

class LocationInfo(WildModel):
    dpu_id = CharField(null = True)
    dpu_location = CharField(null = True)
    class Meta:
        db_table = 'cloud_location_info'

class ThresholdConfigInfo(WildModel):
    site_id = CharField(null = False)
    dpu_id = CharField(null = False)
    dpu_location = CharField(null = False)
    ilf_threshold_warning = FloatField(null = False)
    ilf_threshold_critical = FloatField(null = False)
    mdil_threshold_warning = FloatField(null = False)
    mdil_threshold_critical = FloatField(null = False)
    class Meta:
        db_table = 'threshold_config_info'


if __name__ == '__main__':
    if Log.logger is None:
      my_log = Log()
    Log.logger.info("mvis_model: main program")

    psql_db.create_tables([TrainProcessedInfo, TrainConsolidatedInfo, EventInfo, ErrorInfo, HealthInfo, LocationInfo, ThresholdConfigInfo, MvisProcessedInfo])
