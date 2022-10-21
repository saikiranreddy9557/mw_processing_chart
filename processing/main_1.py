from config import first_iteration,folder_ids_list
from modules.box_utils import BOX_UTILS
from modules.processing_utils  import extract_data_from_pdf,extract_data_from_excel,delete_file,pre_processing_excel_dict,pre_processing_pdf_dict
from modules.dynamo_utils import AWS_Dynamo_Client
from modules.logging_utils import LoggingUtils
 
import sys
import json



def main():
    logger = LoggingUtils("mw-chart-processing").get_logger()
    # logger.info(f"")
    # logger.error(f"")
    # logger.debug(f"")
    # logger.exception(f"")
    first_iteration = False
    if first_iteration :
        pass
    else:
        try:
            first_iteration = True
            logger.info(f"started reading files from the box folder")

            for each_folder_id in folder_ids_list:
                logger.info(f"making connection to box folder")
                # box_obj = BOX_UTILS()
                # box_con = box_obj.box_auth()
                
                logger.info(f"started reading files from the box folder id {each_folder_id}")

                #returns dict 
                #{"pdf":[(item.id,item.name,folder_name)],"xlsx":[(item.id,item.name,folder_name)]}
                #folder_content = box_obj.box_read_folder_content(box_con,each_folder_id)
                #logger.info(f"completed reading files from the box folder content form folder-id {each_folder_id}")
                # retry =0
                #dynamo_obj = AWS_Dynamo_Client()
                
                pdf = [("item.id","item.name","folder_name")]
                for each_pdf_file_info in pdf:
                    # '1039949940900', '132B7467P001-20180719MW report.xlsx', 'test-excel-1'
                    #logger.info(f"started reading data from the id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    # retry = 0
                    
                    #download the file to read it
                    #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                    #pdf_file_path ,file_data= box_obj.download_file(box_con,each_pdf_file_info)
                    
                    pdf_file_path = r'C:\Users\703324564\Desktop\MW_CHART_PROCESSING\files\pdf\MW --423896001-1   7F High Output R0.pdf'

                    file_data= {"folder_name":"test-pdf-1","file_name":"MW --423896001-1   7F High Output R0.pdf" ,"created_at":"2022-10-14 13:33:29" }
                    
                    #returns dict containign all the data in file,file_path
                    file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                    
                    #logger.info(f"completed reading data from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #load the data to dynamodb table

                    # dynamo_obj.push_data(file_data)

                    f_data = pre_processing_pdf_dict(file_data)

                    # Serializing json
                    json_object = json.dumps(f_data, indent=4)
                    
                    # Writing to sample.json
                    with open("dynamo_data_MW --423896001-1   7F High Output R0_pdf.json", "w") as outfile:
                        outfile.write(json_object)
                    

                    pdf_file_path = r'C:\Users\703324564\Desktop\MW_CHART_PROCESSING\files\pdf\MW---423839925-2--7F HIFLOW R0  --1.pdf'

                    file_data= {"folder_name":"test-pdf-1","file_name":"MW---423839925-2--7F HIFLOW R0  --1.pdf" ,"created_at":"2022-10-14 13:33:29" }
                    
                    #returns dict containign all the data in file,file_path
                    file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                    
                    #logger.info(f"completed reading data from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")

                    #preprocess pdf dict data

                    f_data = pre_processing_pdf_dict(file_data)


                    #load the data to dynamodb table

                    # dynamo_obj.push_data(file_data)

                   

                    # Serializing json
                    json_object = json.dumps(f_data, indent=4)
                    
                    # Writing to sample.json
                    with open("dynamo_data_MW---423839925-2--7F HIFLOW R0  --1_pdf.json", "a") as outfile:
                        outfile.write(json_object)
                    
                    #logger.info(f"psuhed data  from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]} to dynamo db")
                    #logger.info(f"succefull read and upload of file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #delete file downloaded
                    # if delete_file(pdf_file_path):
                    #     pass

                    #logger.info(f"succefull read and upload of data from folder {each_folder_id}")
                    


                for each_excel_file_info in folder_ids_list:


                    #logger.info(f"started reading data from the id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]}")
                    #download the file to read it
                    #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                    #excel_file_path ,file_data= box_obj.download_file(box_con,each_excel_file_info)
        
                    excel_file_path = r'C:\Users\703324564\Desktop\MW_CHART_PROCESSING\files\excel\132B7467P001-20180719MW report.xlsx'

                    file_data= {"folder_name":"test-excel-1","file_name":"132B7467P001-20180719MW report.xlsx" ,"created_at":"2022-10-14 13:33:29" }

                    #returns dict containign all the data in file,file_path
                    file_data,file_path = extract_data_from_excel(excel_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                    
                    #logger.info(f"completed reading data from the file-id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]}")

                    #preproces excel dict data

                    f_data = pre_processing_excel_dict(file_data)

                    #load the data to dynamodb table

                    # dynamo_obj.push_data(file_data)

                     # Serializing json
                    json_object = json.dumps(f_data, indent=4)
                    
                    # Writing to sample.json
                    with open("dynamo_data_132B7467P001-20180719MW report_xlsx'.json", "a") as outfile:
                        outfile.write(json_object)

                    #logger.info(f"psuhed data  from the file-id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]} to dynamo db")

                    #logger.info(f"succefull read and upload of file-id- {each_excel_file_info[0]} file-name -{each_excel_file_info[1]} vendor-name -{each_excel_file_info[2]}")
                    #delete file downloaded
                    # if delete_file(file_path):
                    #     pass


                    #logger.info(f"succefull read and upload of data from folder {each_folder_id}")

            return True
        except Exception as e:
            print(e)



if __name__ == "__main__":
    try:
        main()
    except:
        sys.exit()
