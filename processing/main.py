from config import first_iteration,folder_ids_list
from modules.box_utils import BOX_UTILS
from modules.processing_utils  import extract_data_from_pdf,extract_data_from_excel,delete_file
from modules.dynamo_utils import AWS_Dynamo_Client
from modules.loggin_utils import LoggingUtils
 
import sys



def main():
    logger = LoggingUtils("mw-chart-processing-logs").get_logger()
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
                box_obj = BOX_UTILS()
                box_con = box_obj.box_auth()
                
                logger.info(f"started reading files from the box folder id {each_folder_id}")

                #returns dict 
                #{"pdf":[(item.id,item.name,folder_name)],"xlsx":[(item.id,item.name,folder_name)]}
                folder_content = box_obj.box_read_folder_content(box_con,each_folder_id)
                logger.info(f"completed reading files from the box folder content form folder-id {each_folder_id}")
                # retry =0
                #dynamo_obj = AWS_Dynamo_Client()
                

                for each_pdf_file_info in folder_content["pdf"]:
                    # '1039949940900', '132B7467P001-20180719MW report.xlsx', 'test-excel-1'
                    logger.info(f"started reading data from the id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    # retry = 0
                    
                    #download the file to read it
                    #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                    pdf_file_path ,file_data= box_obj.download_file(box_con,each_pdf_file_info)
                    
                

                    #returns dict containign all the data in file,file_path
                    file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                    
                    logger.info(f"completed reading data from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #load the data to dynamodb table

                    # dynamo_obj.push_data(file_data)
                    
                    logger.info(f"psuhed data  from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]} to dynamo db")
                    logger.info(f"succefull read and upload of file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #delete file downloaded
                    if delete_file(pdf_file_path):
                        pass

                    logger.info(f"succefull read and upload of data from folder {each_folder_id}")


                for each_excel_file_info in folder_content["xlsx"]:


                    logger.info(f"started reading data from the id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #download the file to read it
                    #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                    excel_file_path ,file_data= box_obj.download_file(box_con,each_excel_file_info)
        
                

                    #returns dict containign all the data in file,file_path
                    file_data,file_path = extract_data_from_excel(excel_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                    
                    logger.info(f"completed reading data from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")

                    #load the data to dynamodb table

                    # dynamo_obj.push_data(file_data)


                    logger.info(f"psuhed data  from the file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]} to dynamo db")

                    logger.info(f"succefull read and upload of file-id- {each_pdf_file_info[0]} file-name -{each_pdf_file_info[1]} vendor-name -{each_pdf_file_info[2]}")
                    #delete file downloaded
                    if delete_file(file_path):
                        pass


                    logger.info(f"succefull read and upload of data from folder {each_folder_id}")

            return True
        except Exception as e:
            print(e)



if __name__ == "__main__":
    try:
        main()
    except:
        sys.exit()
     

