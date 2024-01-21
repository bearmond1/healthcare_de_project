import datetime as dt
import os
import csv
from sqlalchemy import create_engine
import dateutil.parser as parser

# db
connstr = "mysql+mysqlconnector://root:password@localhost:3306/healthcare"
create_table_stmnt = """
    CREATE TABLE `healthcare`.`healthcare_data` (
      `id` INT NOT NULL AUTO_INCREMENT,
      `Name` VARCHAR(45) NULL,
      `Age` INT NULL,
      `Gender` VARCHAR(45) NULL,
      `Bloodtype` VARCHAR(45) NULL,
      `Medical_Condition` VARCHAR(45) NULL,
      `Admission_date` DATE NULL,
      `Doctor` VARCHAR(45) NULL,
      `Hospital` VARCHAR(45) NULL,
      `Insurance_Provider` VARCHAR(45) NULL,
      `Bill` DECIMAL(10,5) NULL,
      `Room_Number` INT NULL,
      `Admission_Type` VARCHAR(45) NULL,
      `Discharge_Date` DATE NULL,
      `Medication` VARCHAR(45) NULL,
      `Test_Results` VARCHAR(45) NULL,
      PRIMARY KEY (`id`));
 """

def format_row(row):
    row[5] = parser.parse(row[5]).strftime('%Y-%m-%d %H:%M:%S')
    row[12] = parser.parse(row[12]).strftime('%Y-%m-%d %H:%M:%S')
    row[10] = int(row[10])
    row[9] = float(row[9])
    row[1] = int(row[1])
    return row


os.chdir('C:\Projects\healthcare\data_source\source_1')
curdir = os.getcwd()

files = os.listdir()
files_content = []
for file in files:
    new_file = True
    with open(file,'r') as my_file:
        spamreader = csv.reader(my_file, delimiter=',')
        file_content = []
        for row in spamreader:
            if new_file == True:
                new_file = False
                continue
            file_content.append(format_row(row))
        #file_content = [ format_row(row) for row in spamreader ]
        file_content = str(file_content).replace('[','(').replace(']',')')[1:len(str(file_content))-1]
        files_content.append(file_content)
    #break

stmnt = """insert into healthcare.healthcare_data
            (Name,Age,Gender,Bloodtype,Medical_Condition,Admission_date,Doctor,Hospital,Insurance_Provider,Bill,Room_Number,Admission_Type,Discharge_Date,Medication,Test_Results)
            values 
            """
for file in files_content:
    stmnt = stmnt + file + ','
stmnt = stmnt[:len(stmnt)-1]
stmnt = stmnt + ';'

engine = create_engine(connstr)
with engine.connect() as conn:
    conn.execute("truncate healthcare.healthcare_data;")
    result = conn.execute(stmnt)