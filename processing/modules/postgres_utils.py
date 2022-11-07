import psycopg2

import os
import sys
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

sys.path.append(BASE_DIR)

import config

from datetime import datetime
  
# # ct stores current time
# ct = datetime.datetime.now().replace(microsecond=0)
# print("current time:-", ct)
  

def Db_Connection():
    try:
        connection = psycopg2.connect(host=config.host,database=config.database,user=config.user,password=config.password,port = "5432")
        print("connection made succesfully")
        return connection
    except Exception as e:
        print(e)
    

def check_table_empty(db_connection):
    try:
        connection = db_connection
        cursor = connection.cursor()
        q_string="select exists (select 1 from {})".format(config.table)
        print(q_string)
        val=cursor.execute(q_string)
        print(val)
        return val

    except Exception as e:
        print(e)

def insert_log(log_data:list):

    try:
        print("inside the insert log")
        print(log_data)
        connection = Db_Connection()
        cursor = connection.cursor()

        upsert_query = """INSERT INTO mw_chart_processed_1(file_id, file_name,file_vendor_name ,file_uploaded, message) VALUES (%s, %s, %s, %s, %s) ON CONFLICT(file_id) DO UPDATE SET file_id = EXCLUDED.file_id ,file_name = EXCLUDED.file_name , file_vendor_name = EXCLUDED.file_vendor_name, file_uploaded = EXCLUDED.file_uploaded , message = EXCLUDED.message ;"""

        # record_to_insert = (121211213,,"dwddsd.pdf",True,"uploaded succesfully to s3 and dynamo","vendorname")
        record_to_insert = tuple(log_data)
        cursor.execute(upsert_query,record_to_insert)
        connection.commit()
    except Exception as e:
        print(e)



def update_previous_timestamp(execution_id,execution_time_stamp):

    try:
       
        connection = Db_Connection()
        cursor = connection.cursor()

        insert_query = """	
                        INSERT INTO public.mw_chart_process_previous_1(
                            execution_id, previous_timestamp)
                            VALUES (%s,%s);"""

        # record_to_insert = (121211213,,"dwddsd.pdf",True,"uploaded succesfully to s3 and dynamo","vendorname")
      
        l = [execution_id,execution_time_stamp]
        record_to_insert = tuple(l)
        cursor.execute(insert_query,record_to_insert)
        connection.commit()
    except Exception as e:
        print(e)

def select_previous_timestamp():
  

    try:
        connection = Db_Connection()
        cursor = connection.cursor()
        print("inside previsu time stmap")
        
        query = """ SELECT  previous_timestamp FROM public.mw_chart_process_previous_1 ORDER BY previous_timestamp DESC LIMIT 1 ; """
        
        cursor.execute(query)
        result = cursor.fetchone();
        print(result)
        if result:
            prev_timestamp = result[0].strftime("%Y-%m-%d %H:%M:%S")

            return prev_timestamp
        else:
            current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
            current_timestamp = current_timestamp+" 00:00:00"
            return current_timestamp

    except Exception as e:
        print(e)
    

# import uuid

# e_id = str(uuid.uuid4())



import datetime  
# # timestamp = 1625309472.357246 
# # date_time = datetime.fromtimestamp(timestamp)

ct = datetime.datetime.now().strftime("%Y-%m-%d")
print(type(ct))
ct = ct+" 00:00:00"
print(ct)
# str_date_time = ct.strftime("%Y-%m-%d %H:%M:%S")

# print(str_date_time)

# update_previous_timestamp(e_id,str(str_date_time))
# v = select_previous_timestamp()
# print(v)
# str_date_time = v[0].strftime("%Y-%m-%d %H:%M:%S")
# print(str_date_time)
# print(type(v[0]))
# print(str(v))

# print(v[0])

# TODAY_CHECK = datetime.datetime.strptime(v[0], "%Y-%m-%d %H:%M:%S")
# print(TODAY_CHECK)




# P_insert_data = """INSERT INTO mw_chart_processed(
# 	file_id, file_hash_id, file_name, file_uploaded, message)
# 	VALUES (%s, %s, %s, %s, %s);"""
# record_to_insert = (12121121,"2121dsfsadad","dwddsd.pdf",True,"uploaded succesfully to s3 and dynamo")



# db = Db_Connection()

# cursor = db.cursor()
# import time

# t = time.time()
# from datetime import datetime  
# timestamp = 1625309472.357246 
# date_time = datetime.fromtimestamp(timestamp)
# str_date_time = date_time.strftime("%d-%m-%Y, %H:%M:%S")
# print("Current timestamp", str_date_time)
# postgres_insert_query = """ INSERT INTO mw_chart_processed (id, previous_processed, procssed_id) VALUES (%s,%s,%s)"""
# record_to_insert = (5, "03-07-2021 16:21:12", 950)
# cursor.execute(postgres_insert_query, record_to_insert)
# print("scuccess")

# print(check_table_empty(db))
# # # "2022-10-10"

# def get_previous_processed_timestamp(db_connection):
#     ct = datetime.datetime.now()
# # print("current time:-", ct)
        


