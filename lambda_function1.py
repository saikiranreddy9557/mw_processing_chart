import os
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),))
print(BASE_DIR)

from processing.config import first_iteration,folder_ids_list
from processing.modules.box_utils import BOX_UTILS
from processing.modules.processing_utils  import extract_data_from_pdf,extract_data_from_excel,delete_file,pre_processing_excel_dict,pre_processing_pdf_dict
from processing.modules.dynamo_utils import AWS_Dynamo_Client
from processing.modules.logging_utils import LoggingUtils
 
import sys
import json
pdf_path = BASE_DIR+"/files/pdf/"
excel_path = BASE_DIR+"/files/excel/"

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    
    
    first_iteration = False
    if first_iteration :
        pass
    else:
        try:
            first_iteration = True
            logger.info(f"started reading files from the box folder")

            # for each_folder_id in folder_ids_list:
            logger.info(f"making connection to box folder")
            
            each_folder_id = "1212121212122"
            logger.info(f"started reading files from the box folder id {each_folder_id}")

            
            dynamo_obj = AWS_Dynamo_Client()
            
            # pdf = [("item.id","item.name","folder_name")]
            # for each_pdf_file_info in pdf:
                
                
            pdf_file_path = pdf_path+'MW --423896001-1   7F High Output R0.pdf'

            file_data= {"folder_name":"test-pdf-1","file_name":"MW --423896001-1   7F High Output R0.pdf" ,"created_at":"2022-10-14 13:33:29" }
            
            #returns dict containign all the data in file,file_path
            file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
            
            

            f_data = pre_processing_pdf_dict(file_data)
            dynamo_obj.push_data(f_data)
            print("suceess")

            #Serializing json
            json_object = json.dumps(f_data, indent=4)
            
            #Writing to sample.json
            with open("dynamo_data_MW --423896001-1   7F High Output R0_pdf.json", "w") as outfile:
                outfile.write(json_object)
            

            pdf_file_path = pdf_path+'MW---423839925-2--7F HIFLOW R0  --1.pdf'

            file_data= {"folder_name":"test-pdf-1","file_name":"MW---423839925-2--7F HIFLOW R0  --1.pdf" ,"created_at":"2022-10-14 13:33:29" }
            
            #returns dict containign all the data in file,file_path
            file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
    

            f_data = pre_processing_pdf_dict(file_data)
            dynamo_obj.push_data(f_data)
            print("suceess")

            # Serializing json
            json_object = json.dumps(f_data, indent=4)
            
            # Writing to sample.json
            with open("dynamo_data_MW---423839925-2--7F HIFLOW R0  --1_pdf.json", "a") as outfile:
                outfile.write(json_object)

            excel_file_path = excel_path+'132B7467P001-20180719MW report.xlsx'

            file_data= {"folder_name":"test-excel-1","file_name":"132B7467P001-20180719MW report.xlsx" ,"created_at":"2022-10-14 13:33:29" }

            #returns dict containign all the data in file,file_path
            file_data,file_path = extract_data_from_excel(excel_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
            
            

            f_data = pre_processing_excel_dict(file_data)

            #load the data to dynamodb table

            dynamo_obj.push_data(f_data)
            print("suceess")

            # Serializing json
            json_object = json.dumps(f_data, indent=4)
            
            # Writing to sample.json
            with open("dynamo_data_132B7467P001-20180719MW report_xlsx'.json", "a") as outfile:
                outfile.write(json_object)



            logger.info(f"succefull read and upload of data from folder {each_folder_id}")

            return True
        except Exception as e:
            print(e)



if __name__ == "__main__":
    try:
        lambda_handler("ai","kiran")
    except:
        sys.exit()
