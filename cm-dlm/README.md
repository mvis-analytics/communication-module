Install following packages:
--------------------------

#POSTGRESQL installation in UBUNTU:

https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04

#Python3 packages installation:

- pip3 install peewee
- pip3 install psycopg2
- pip3 install psycopg2cffi

#Python code:

./cm-dlm/config:
------------------
- ../../config/wild_dlm.conf : contains configuration parameters of DLM module.

./cm-dlm/wild_dlm/src:
--------
- wild_dlm_conf.py : contains class to read and load configuration file.

- wild_dlm_main.py : contains main dlm application code.

- wild_model.py : contains wild database object relational model.

- wild_api.py: contains wild database rest api.

- wild_dlm_mqtt.py: contains class to publish dlm event/error messages. 

./cm-dlm/wild_dlm/test:
--------
- publish-healthinfo.py : script to publish wild Health information message locally.

- publish-train-proc-info.py: script to publish Train processed information message locally. 

- publish-train-con-info.py: script to publish Train consolidated information message locally.

- publish-eventinfo.py: script to publish Event information message locally.

- publish-errorinfo.py: Script to publish Error information message locally.

./cm-dlm/wild_common:
--------
- MqttClient.py : Mqtt client library
- wild_log.py :   Logger library
