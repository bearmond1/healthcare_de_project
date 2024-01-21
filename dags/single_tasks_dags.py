from __future__ import annotations

import logging
import sys
import time
from pprint import pprint
import requests
from sqlalchemy import create_engine
import datetime as dt
import os
from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed
import csv
import pendulum
import connections as cns


@task(task_id="stage_files")
def stage_files(ds=None, **kwargs):
        os.chdir('exchange_files/source_2')
        curdir = os.getcwd() 

        files = os.listdir()
        with cns.get_sf_conn().cursor() as cur:
            for file in files:
                cur.execute(f"put file:///{curdir + '/' + file} @HC_FILES_STAGE;")
                
        return
        
        
        
        filename_wo_e = str(dt.date.today().year - 3)
        ext = '.csv'
        filename = filename_wo_e + ext
        file_copy = filename_wo_e + '_copy' + ext
        if file_copy in files:
            os.remove(file_copy)
        if not filename in files:
            return
        # shorten file for test
        with open(filename, 'r') as source_file:
            source_reader = csv.reader(source_file, delimiter=',',quotechar='"')
            with open(file_copy, 'w') as copy_file:
                copy_writer = csv.writer(copy_file, delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for i,row in enumerate(source_reader):
                    if i == 0:
                        continue
                    if i > 10:
                        break
                    copy_writer.writerow(row)

        with cns.get_sf_conn().cursor() as cur:
            cur.execute(f'REMOVE @HC_FILES_STAGE/{file_copy};')
            cur.execute(f"put file:///{curdir + '/' + file_copy} @HC_FILES_STAGE;")
        files = os.listdir()
        if file_copy in files:
            os.remove(file_copy)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def stage_files_dag():        
    stage_files()

stage_files_dag()



@task(task_id="ingest_from_local_db")
def ingest_from_db(ds=None, **kwargs):
    engine = create_engine(cns.source_db_connstr)
    with engine.connect() as conn:
        stmnt = f"""
                select  
                Age,Gender,Bloodtype,Medical_Condition,Admission_date,Hospital,Insurance_Provider,Bill,Room_Number,Admission_Type,Discharge_Date,Medication,Test_Results
                from healthcare.healthcare_data
                """
                #where EXTRACT(YEAR FROM Admission_date) = {dt.date.today().year-1}
                #limit 10
        result = conn.execute(stmnt)
        if result.rowcount == 0:
            return
    insert_stmnt = """
            insert into HEALTHCARE.SOURCE_DATA.HC_DATA(Id,Age,Gender,Bloodtype,Medical_Condition,Admission_date,Hospital,Insurance_Provider,Bill,Room_Number,Admission_Type,Discharge_Date,Medication,Test_Results,DATA_SOURCE,UPLOAD_TS)
            values 
        """
    for row in result:
        value = f"""(HEALTHCARE.SOURCE_DATA.HC_DATA_PK_SEQ.nextval,{row[0]}, '{row[1]}','{row[2]}','{row[3]}','{str(row[4])}','{row[5]}','{row[6]}',{float(row[7])},{int(row[8])},'{row[9]}','{str(row[10])}','{row[11]}','{row[12]}','database','{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}')"""
        insert_stmnt = insert_stmnt + value + ','
    insert_stmnt = insert_stmnt[:len(insert_stmnt)-1]
    print(insert_stmnt)
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('use role accountadmin;')
        cur.execute(insert_stmnt)

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def ingest_from_db_dag():
    ingest_from_db()

ingest_from_db_dag()



@task(task_id="trigger_sf_task")
def trigger_sf_task(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.SOURCE_DATA.load_hc_data_from_stage();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_task_dag():
    trigger_sf_task()

trigger_sf_task_dag()


# Date Dimension
@task(task_id="trigger_sf_update_date_dim")
def trigger_sf_update_date_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_DATE_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_date_dim_dag():
    trigger_sf_update_date_dim()

trigger_sf_update_date_dim_dag()


# Gender Dimension
@task(task_id="trigger_sf_update_gender_dim")
def trigger_sf_update_gender_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.update_gender_dim();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_gender_dim_dag():
    trigger_sf_update_gender_dim()

trigger_sf_update_gender_dim_dag()


# Bloodtype Dimension
@task(task_id="trigger_sf_update_bloodtype_dim")
def trigger_sf_update_bloodtype_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_Bloodtype_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_bloodtype_dim_dag():
    trigger_sf_update_bloodtype_dim()

trigger_sf_update_bloodtype_dim_dag()


# Medical Condition Dimension
@task(task_id="trigger_sf_update_medical_condition_dim")
def trigger_sf_update_medical_condition_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_MED_CONDITION_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_medical_condition_dim_dag():
    trigger_sf_update_medical_condition_dim()

trigger_sf_update_medical_condition_dim_dag()


# Hospital Dimension
@task(task_id="trigger_sf_update_hospital_dim")
def trigger_sf_update_hospital_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_HOSPITAL_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_hospital_dim_dag():
    trigger_sf_update_hospital_dim()

trigger_sf_update_hospital_dim_dag()


# Insurance Provider Dimension
@task(task_id="trigger_sf_update_insurance_provider_dim")
def trigger_sf_update_insurance_provider_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_insurance_provider_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_insurance_provider_dim_dag():
    trigger_sf_update_insurance_provider_dim()

trigger_sf_update_insurance_provider_dim_dag()


# Admission Type Dimension
@task(task_id="trigger_sf_update_admission_type_dim")
def trigger_sf_update_admission_type_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_admission_type_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_admission_type_dim_dag():
    trigger_sf_update_admission_type_dim()

trigger_sf_update_admission_type_dim_dag()


# Medication Dimension
@task(task_id="trigger_sf_update_medication_dim")
def trigger_sf_update_medication_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_medication_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_medication_dim_dag():
    trigger_sf_update_medication_dim()

trigger_sf_update_medication_dim_dag()


# Test Results Dimension
@task(task_id="trigger_sf_update_Test_Results_dim")
def trigger_sf_update_Test_Results_dim(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_Test_Results_DIM();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_update_Test_Results_dim_dag():
    trigger_sf_update_Test_Results_dim()

trigger_sf_update_Test_Results_dim_dag()


# Fact Table 
@task(task_id="trigger_sf_upload_fact_table")
def trigger_sf_upload_fact_table(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute('CALL HEALTHCARE.mart.UPDATE_FACT_TABLE();')

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def trigger_sf_upload_fact_table_dag():
    trigger_sf_upload_fact_table()

trigger_sf_upload_fact_table_dag()


# Truncate All
@task(task_id="truncate_all")
def truncate_all(ds=None, **kwargs):
    with cns.get_sf_conn().cursor() as cur:
        cur.execute("truncate healthcare.source_data.hc_data;")
        cur.execute("truncate healthcare.mart.admission_type_dim;")
        cur.execute("truncate healthcare.mart.bloodtype_dim;")
        cur.execute("truncate healthcare.mart.date_dim;")
        cur.execute("truncate healthcare.mart.gender_dim;")
        cur.execute("truncate healthcare.mart.hospital_dim;")
        cur.execute("truncate healthcare.mart.insurance_provider_dim;")
        cur.execute("truncate healthcare.mart.medical_admissions_fact_table;")
        cur.execute("truncate healthcare.mart.medication_dim;")
        cur.execute("truncate healthcare.mart.med_condition_dim;")
        cur.execute("truncate healthcare.mart.test_results_dim;")

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def truncate_all_dag():
    truncate_all()

truncate_all_dag()