
from processing.config import first_iteration,folder_ids_list,bucket_name,region
from processing.modules.box_utils import BOX_UTILS
from processing.modules.processing_utils  import extract_data_from_pdf,extract_data_from_excel,delete_file,pre_processing_excel_dict,pre_processing_pdf_dict,validation_check
from processing.modules.dynamo_utils import AWS_Dynamo_Client
from processing.modules.logging_utils import LoggingUtils
from processing.modules.postgres_utils import  insert_log,select_previous_timestamp,update_previous_timestamp

import sys

import uuid

import logging

import os
import sys
import psycopg2
import uuid
from datetime import datetime



logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    
    first_iteration= None
  
   
    try:
        first_iteration = True
        logger.info(f"started reading files from the box folder")
        execution_id = str(uuid.uuid4())
        previous_timestamp = select_previous_timestamp()
        for each_folder_id in folder_ids_list:
            logger.info(f"making connection to box folder")
            box_obj = BOX_UTILS(start_date="2022-10-10",end_date="2022-10-17")
            box_con = box_obj.box_auth()
            
            logger.info(f"started reading files from the box folder id {each_folder_id}")

            #returns dict 
            #{"pdf":[(item.id,item.name,folder_name)],"xlsx":[(item.id,item.name,folder_name)]}
            folder_content = box_obj.box_read_folder_content(box_con,each_folder_id,previous_timestamp)
            logger.info(f"completed reading files from the box folder content form folder-id {each_folder_id}")

            # retry =0
            dynamo_obj = AWS_Dynamo_Client()
            
            print("folder_content",folder_content)

            for each_pdf_file_info in folder_content["pdf"]:
                # '1039949940900', '132B7467P001-20180719MW report.xlsx', 'test-excel-1'
                logger.info(f"started reading data from the id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                
                # retry = 0
                
                #download the file to read it
                #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                pdf_file_path ,file_data= box_obj.download_file(box_con,each_pdf_file_info)
                
            

                #returns dict containign all the data in file,file_path
                file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                file_data["execution_id"] = execution_id
                if file_data:
                    f_data = pre_processing_pdf_dict(file_data)
                    
                    logger.info(f"completed reading data from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #load the data to dynamodb table
                    
                    dynamo_obj.push_data(f_data)

                    

                    
                    dynamo_obj.upload_to_aws_s3(file_path, bucket_name, f_data["ChartFileName"],execution_id = execution_id)
                     
                    logger.info(f"psuhed data  from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]} to dynamo db")
                    logger.info(f"succefull read and upload of file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")


                    #file_id, , file_name, file_uploaded:Bool, message,file_vendor_name
                    file_meta_data = [each_pdf_file_info[0],each_pdf_file_info[1],each_pdf_file_info[2],True,"uploaded succesfully"]

                    #insert log in db table
                    insert_log(file_meta_data)
                    #delete file downloaded
                else:
                    logger.info(f"file not in desired format")
                    logger.info(f"unsuccefull read and upload of file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]} ")
                    file_meta_data = [each_pdf_file_info[0],each_pdf_file_info[1],each_pdf_file_info[2],False,"uploaded unsuccesfully file not in desired format"]
                    insert_log(file_meta_data)
                if delete_file(pdf_file_path):
                    pass

                logger.info(f"succefull read and upload of data from folder {each_folder_id}")


            for each_excel_file_info in folder_content["xlsx"]:


                logger.info(f"started reading data from the id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]}")
                #download the file to read it
                #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                excel_file_path ,file_data= box_obj.download_file(box_con,each_excel_file_info)
    
            

                #returns dict containign all the data in file,file_path
                file_data,file_path = extract_data_from_excel(excel_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                file_data["execution_id"] = execution_id
                if file_data:
                    f_data = pre_processing_excel_dict(file_data)
                    
                    logger.info(f"completed reading data from the file-id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]}")

                    #load the data to dynamodb table
                    
                    dynamo_obj.push_data(f_data)

                    dynamo_obj.upload_to_aws_s3(file_path, bucket_name, f_data["ChartFileName"],execution_id = execution_id)
                    

                    logger.info(f"psuhed data  from the file-id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]} to dynamo db")

                    logger.info(f"succefull read and upload of file-id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]}")
                    #file_id, file_name, file_vendor_name, file_uploaded, message)
                    file_meta_data = [each_excel_file_info[0],each_excel_file_info[1],each_excel_file_info[2],True,"uploaded succesfully"]

                    #insert log in db table
                    insert_log(file_meta_data)
                else:
                    logger.info(f"file not in desired format")
                    logger.info(f"unsuccefull read and upload of file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]} ")
                    file_meta_data = [each_pdf_file_info[0],each_pdf_file_info[1],each_pdf_file_info[2],False,"uploaded unsuccesfully file not in desired format"]
                    insert_log(file_meta_data)
                
                #delete file downloaded
                if delete_file(file_path):
                    pass


            logger.info(f"succefull read and upload of data from folder {each_folder_id}")

           
        print('near erro')
        execution_end_timestmap = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        update_previous_timestamp(execution_id,execution_end_timestmap)

        return True
    except Exception as e:
        print(e)

        sys.exit()



lambda_handler("event","context")

