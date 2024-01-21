from __future__ import annotations

import logging
import sys
import time
from pprint import pprint
import requests
from sqlalchemy import create_engine
import datetime as dt
import os
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed
import snowflake.connector
import connections as cns
import single_tasks_dags
from single_tasks_dags import *
import csv



@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def full_dag():      
    stage_files_t = stage_files()       
    trigger_sf_stage_upload_t = trigger_sf_task()
    ingest_sf_from_db_t = ingest_from_db()
    trigger_sf_update_date_dim_t = trigger_sf_update_date_dim()    
    trigger_sf_update_gender_dim_t = trigger_sf_update_gender_dim()
    trigger_sf_update_bloodtype_dim_t = trigger_sf_update_bloodtype_dim()
    trigger_sf_update_medical_condition_dim_t = trigger_sf_update_medical_condition_dim()
    trigger_sf_update_hospital_dim_t = trigger_sf_update_hospital_dim()
    trigger_sf_update_insurance_provider_dim_t = trigger_sf_update_insurance_provider_dim()
    trigger_sf_update_admission_type_dim_t = trigger_sf_update_admission_type_dim()
    trigger_sf_update_medication_dim_t = trigger_sf_update_medication_dim()
    trigger_sf_update_Test_Results_dim_t = trigger_sf_update_Test_Results_dim()
    trigger_sf_upload_fact_table_t = trigger_sf_upload_fact_table()
    
    @task(task_id="source_data_updated_point")
    def source_data_updated_point():
        pass

    source_data_updated_point_t = source_data_updated_point()


    # pipeline
    stage_files_t >> trigger_sf_stage_upload_t
    [trigger_sf_stage_upload_t, ingest_sf_from_db_t] >> source_data_updated_point_t >> [trigger_sf_update_date_dim_t, trigger_sf_update_gender_dim_t, trigger_sf_update_bloodtype_dim_t, trigger_sf_update_medical_condition_dim_t, trigger_sf_update_hospital_dim_t, trigger_sf_update_insurance_provider_dim_t, trigger_sf_update_insurance_provider_dim_t, trigger_sf_update_admission_type_dim_t, trigger_sf_update_medication_dim_t, trigger_sf_update_Test_Results_dim_t] >> trigger_sf_upload_fact_table_t

full_dag()