import datetime as dt
import os
import csv
from sqlalchemy import create_engine
import dateutil.parser as parser
import snowflake.connector
from snowflake.core import Root
import requests

# SF
account = "QXCPTMD-VB14182"
user = "DEMHCP"
password = "Qwerty123"
warehouse = "WH"
database = "HEALTHCARE"
schema = "SOURCE_DATA"

connstr = "mysql+mysqlconnector://root:password@localhost:3306/healthcare"

def get_sf_conn():
    conn = snowflake.connector.connect(
                    account = account,
                    user = user,
                    password = password,            
                    warehouse=warehouse,
                    database=database,
                    schema = schema
                )
    return conn


def insert_data_into_sf_from_files():
    engine = create_engine(connstr)
    with engine.connect() as conn:
        stmnt = f"""
                select  
                Age,Gender,Bloodtype,Medical_Condition,Admission_date,Hospital,Insurance_Provider,Bill,Room_Number,Admission_Type,Discharge_Date,Medication,Test_Results
                from healthcare.healthcare_data
                where EXTRACT(YEAR FROM Admission_date) = {dt.date.today().year-1}
                limit 300
                """
        result = conn.execute(stmnt)
        insert_stmnt = f"""
                insert into HEALTHCARE.SOURCE_DATA.HC_DATA(ID,Age,Gender,Bloodtype,Medical_Condition,Admission_date,Hospital,Insurance_Provider,Bill,Room_Number,Admission_Type,Discharge_Date,Medication,Test_Results,DATA_SOURCE,UPLOAD_TS)
                values 
            """
        for row in result:
            value = f"""(HEALTHCARE.SOURCE_DATA.HC_DATA_PK_SEQ.nextval,{row[0]}, '{row[1]}','{row[2]}','{row[3]}','{row[4].strftime("%Y-%m-%d")}','{row[5]}','{row[6]}',{float(row[7])},{int(row[8])},'{row[9]}','{row[10].strftime("%Y-%m-%d")}','{row[11]}','{row[12]}','database' ,'{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}'),"""
            insert_stmnt = insert_stmnt + value
        insert_stmnt = insert_stmnt[:len(insert_stmnt)-1]
        print(insert_stmnt)
        with get_sf_conn().cursor() as cur:
            cur.execute(insert_stmnt)




def insert_holidays_data():    
    url_template = 'https://date.nager.at/api/v3/publicholidays/'

    os.chdir('C:\Projects\healthcare\data_source\source_1')
    files = os.listdir()
    if len(files) == 0:
        return
    insert_stmnt = f"""
                insert into HEALTHCARE.SOURCE_DATA.holidays_info(date_,holiday_name,local_name)
                values 
            """
    for file in files:
        url = url_template + file[:4] + '/US'
        response = requests.get(url)
        for val in response.json():
            insert_stmnt = insert_stmnt + "('" + val['date'] + "','" + val['name'].replace('\'',"\\'") + "','" + val['localName'].replace('\'',"\\'") + "'),"
    insert_stmnt = insert_stmnt[:len(insert_stmnt)-1]
    print(insert_stmnt)
    with get_sf_conn().cursor() as cur:
        cur.execute(insert_stmnt)

insert_data_into_sf_from_files()