from config import first_iteration,folder_ids_list
from modules.box_utils import BOX_UTILS
from modules.processing_utils  import extract_data_from_pdf,extract_data_from_excel,delete_file
from modules.dynamo_utils import AWS_Dynamo_Client
 
import sys



def main():
    first_iteration = False
    if first_iteration :
        pass
    else:
        first_iteration = True
        for each_folder_id in folder_ids_list:
            box_obj = BOX_UTILS()
            box_con = box_obj.box_auth()

            #returns dict 
            #{"pdf":[(item.id,item.name,folder_name)],"xlsx":[(item.id,item.name,folder_name)]}
            folder_content = box_obj.box_read_folder_content(box_con,each_folder_id)
            # retry =0
            #dynamo_obj = AWS_Dynamo_Client()
            print(folder_content)

            for each_pdf_file_info in folder_content["pdf"]:
                # retry = 0
                
                #download the file to read it
                #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                pdf_file_path ,file_data= box_obj.download_file(box_con,each_pdf_file_info)
                
                print("pdf_path")
                print(pdf_file_path)

                #returns dict containign all the data in file,file_path
                file_data,file_path = extract_data_from_pdf(pdf_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                
                print(file_data)
                print(file_path)
                #load the data to dynamodb table

                # dynamo_obj.push_data(file_data)


                #delete file downloaded
                if delete_file(pdf_file_path):
                    pass


            for each_excel_file_info in folder_content["xlsx"]:



                #download the file to read it
                #file_path,file_meta_data={file_id ,file_name,folder_name,created_at,file_name}
                excel_file_path ,file_data= box_obj.download_file(box_con,each_excel_file_info)
      
                print(excel_file_path)

                #returns dict containign all the data in file,file_path
                file_data,file_path = extract_data_from_excel(excel_file_path,folder_name=file_data["folder_name"],file_name=file_data["file_name"],created_at=file_data["created_at"])
                
                print(file_data)
                for key in file_data.keys():
                    print(key)
                print(file_path)

                #load the data to dynamodb table

                # dynamo_obj.push_data(file_data)


                #delete file downloaded
                if delete_file(file_path):
                    pass


    return True



if __name__ == "__main__":
    try:
        main()
    except:
        sys.exit()
     

