import os
import calendar
import pandas as pd
import datetime as dt
from airflow import DAG
from pathlib import Path
from urllib.error import HTTPError
from airflow import AirflowException
from datetime import date,datetime,timedelta
from openexchangerate import OpenExchangeRates
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator



#api requested from https://openexchangerates.org/
api_key = "05fbe5a37e4047f68aa5e0cf619d6061"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

#Based on current month, check if the folder already exist or we'll have to create new folder 
def check_folder(ti):
    currentMonth = calendar.month_name[datetime.now().month]+str(datetime.now().year)
    folder_path = str(currentMonth)
    output_path = folder_path+"/"+str(date.today())
    ti.xcom_push(key='fpath', value=folder_path)
    ti.xcom_push(key='path', value=output_path)
    if os.path.isdir(folder_path):
        return "Folder_Exist"
    else:
        return "No_Such_Folder"
    
    
    #To create new folder in case of first day of new month
def create_folder(ti):
    folder_path = ti.xcom_pull(key='fpath', task_ids='Check_Folder_Availability')
    os.makedirs(folder_path+"/")
   
   #Test connection to openexchangerates 
def test_connection(api_key, ti):   
    try:
        client = OpenExchangeRates(api_key)
        latest = client.latest()
        ti.xcom_push(key='latest', value=latest.dict)
        return "Request_Rates"
    except HTTPError as e:
        ti.xcom_push(key='e_code', value=e.code)
        ti.xcom_push(key='e_msg', value=e.msg)
        return "Connection_Failed"

#In case of connection failure the dag will fail with the details (error code & error message) will be printed in task's logs
def connection_failed(ti):
    e_code = ti.xcom_pull(key='e_code', task_ids='Test_Connection')
    e_msg = ti.xcom_pull(key='e_msg', task_ids='Test_Connection')
    print("Status Code : "+str(e_code))
    print("Error Message : "+e_msg)
    raise AirflowException('Connection Failed')

#get the rate of usd and eur based on egp 
def request_rates(ti):   
    latest = ti.xcom_pull(key='latest', task_ids='Test_Connection')
    egpTousd = 1/latest["EGP"]
    egpToeur = latest["EUR"]/latest["EGP"]
    ti.xcom_push(key='usd', value=egpTousd)
    ti.xcom_push(key='eur', value=egpToeur)

#check if today's file already exist
def check_file_availability(ti):
    output_path = ti.xcom_pull(key='path', task_ids='Check_Folder_Availability')
    path = Path(output_path)
    if path.is_file():
        return "File_Exist"
    else:
        return "Create_New_File"

#read the last value written in the file and check if the the rates changes or not
def read_last_values(ti):
    egpTousd=ti.xcom_pull(key='usd', task_ids='Request_Rates')
    egpToeur=ti.xcom_pull(key='eur', task_ids='Request_Rates')
    output_path = ti.xcom_pull(key='path', task_ids='Check_Folder_Availability')
    df = pd.read_csv(output_path,dtype=str)
    lastusd = df.tail(1)["EGP to USD"].values[0]
    lasteur = df.tail(1)["EGP to EUR"].values[0]
    if (egpTousd == float(lastusd)) and (egpToeur == float(lasteur)):
        return "No_New_Values"
    else:
        return "Write_Exchangerates"

#Write the exchange rates for usd and eur in csv file
def write_exchangerates(ti): 
    current_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    egpTousd=ti.xcom_pull(key='usd', task_ids='Request_Rates')
    egpToeur=ti.xcom_pull(key='eur', task_ids='Request_Rates')
    df = pd.DataFrame([[current_time,egpTousd,egpToeur]],columns = ["timestamp", "EGP to USD", "EGP to EUR"])
    output_path = ti.xcom_pull(key='path', task_ids='Check_Folder_Availability')
    df.to_csv(output_path,index=False, mode='a',header=not os.path.exists(output_path))
  
    
  #Scheduling the pipeline with airflow dag it will run every hour to check if the rates change and write any changes with timestamp  
with DAG("Exchange_Rates_Pipeline", start_date=datetime(2022, 1 ,1), 
    schedule_interval="@hourly", default_args=default_args, catchup=False) as dag:
    
    
    test_connection = BranchPythonOperator(
        task_id = "Test_Connection",
        python_callable = test_connection,
        op_kwargs={'api_key':api_key}
    )
    
    connection_failed = PythonOperator(
        task_id = "Connection_Failed",
        python_callable = connection_failed
    )
    
    request_rates = PythonOperator(
        task_id="Request_Rates",
        python_callable = request_rates
    )
    
    
    check_folder = BranchPythonOperator(
        task_id = "Check_Folder_Availability",
        python_callable = check_folder
    )
    
    folder_exist = DummyOperator(
        task_id = "Folder_Exist"
    )
    
    check_file_availability = BranchPythonOperator(
    task_id = "Check_File_Availability",
    python_callable = check_file_availability
    )
    
    file_exist = DummyOperator(
        task_id = "File_Exist"
    )
    
    create_new_file = DummyOperator(
        task_id = "Create_New_File"
        ,trigger_rule='one_success'
    )
    
    read_last_values = BranchPythonOperator(
    task_id = "Read_Last_Values",
    python_callable = read_last_values
    )
    
    
    no_such_folder = DummyOperator(
        task_id = "No_Such_Folder"
    )
    
    no_new_values = DummyOperator(
        task_id = "No_New_Values"
    )
    
    create_folder = PythonOperator(
        task_id="Create_New_Folder",
        python_callable = create_folder
    )
   
   
    
    write_exchangerates = PythonOperator(
        task_id="Write_Exchangerates",
        python_callable = write_exchangerates 
        ,trigger_rule='one_success'
    )    
    
    
    
    test_connection >> [connection_failed, request_rates]
    request_rates >> check_folder >> [ folder_exist, no_such_folder]
    no_such_folder >> create_folder >> create_new_file >> write_exchangerates
    folder_exist >> check_file_availability >> [ file_exist, create_new_file]  
    create_new_file >> write_exchangerates
    file_exist >> read_last_values >> [no_new_values,write_exchangerates]
    
    
    
    
    
    
    ###
