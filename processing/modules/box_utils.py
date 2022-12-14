from boxsdk import JWTAuth, Client
import os
import sys
import json

BASE_PATH =  os.path.dirname(os.path.dirname( os.path.dirname( __file__ ) ))

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))

sys.path.append(BASE_DIR)

import config
import datetime



# from boxsdk.config import Proxy
# Proxy.URL = "https://182.95.92.65:80"

# from boxsdk.config import Proxy
# Proxy.AUTH = {
#     'user': '',
#     'password': '',
# }



class BOX_UTILS:
    """class to connect and perfrom opertions on box folder
    """

    def __init__(self,start_date=None,end_date=None,user_id=config.user_id) :
        """[constructor]
        Args:
            user_id ([str], optional): [description]. Defaults to config.user_id.
        """
        self.cred_path = BASE_PATH+r"\config.json"
        # self.cred_path= r"C:\Users\703324564\Desktop\MW_CHART_PROCESSING\config.json"
        self.user_id = user_id
        self.start_date = None
        self.end_date = None

    def box_auth(self,user_id=None):

        """[authenticationg to box folder]
        Returns:
            [str]: [user id of the user authenticating]
        """
        try:
            auth = JWTAuth.from_settings_file(self.cred_path)
            client = Client(auth)
            # 21158211249
            # service_account = client.user().get()
            # print(f'Service Account user ID is {service_account.id}')
            if user_id :
                user_to_impersonate = client.user(user_id=self.user_id)
                user_client = client.as_user(user_to_impersonate)
                return user_client
            else:
                
                user_to_impersonate = client.user(user_id=self.user_id)
                user_client = client.as_user(user_to_impersonate)
                return user_client

        except Exception as e:
            print(e)
            raise e

    def box_file_in_data_range(self,user_client,file_id,previous_timestamp=None):
        file = user_client.file(file_id).get()
        #2022-10-13 23:05:25

        created_at = file.response_object["created_at"].split("T")
        created_at[1] = created_at[1][:8]
        created_at = " ".join(created_at)
        

        
        TODAY_CHECK = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        
        start = datetime.datetime.strptime(previous_timestamp, "%Y-%m-%d %H:%M:%S")

       

        if TODAY_CHECK > start:
            return True
        else:
            return False
        
    
    def box_file_in_data_range_legacy(self,user_client,file_id,start,end):
        file = user_client.file(file_id).get()
        #2022-10-13 23:05:25

        created_at = file.response_object["created_at"].split("T")
        created_at[1] = created_at[1][:8]
        created_at = " ".join(created_at)
        

        
        TODAY_CHECK = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

       

        if start <= TODAY_CHECK <= end:
            return True
        else:
            return False


    def box_read_folder_content(self,user_client,folder_id,previous_timestap):


        """[read all content of the folder and return the pdf andd excel files]
        Returns:
            [dict]: [file meta data of both pdf and excel]
        """
        print(previous_timestap)
        print("dddddddddddddddddddddddd")
        folder = user_client.folder(folder_id=folder_id).get()
        folder_name = folder.name
        file_ids={"pdf":[],"xlsx":[]}
        try:
            items = user_client.folder(folder_id=folder_id).get_items()
            for item in items:
                
                # print(f'{item.type.capitalize()} {item.id} is named "{item.name}"')
                if item.name[-3:] == "pdf":
                    print("inside pdf")
                    if self.box_file_in_data_range(user_client,item.id,previous_timestap):
                        file_ids["pdf"].append((item.id,item.name,folder_name)) 
                if item.name[-4:] == "xlsx":
                    
                    print("indies excel")
                    if self.box_file_in_data_range(user_client,item.id,previous_timestap):
                        file_ids["xlsx"].append((item.id,item.name,folder_name))

            return file_ids
        except Exception as e:
            print(e)
            raise e

    def download_file(self,user_client,file_info):
        """[dwonlaoing the file form the box folder at pre defined path]
        Args:
            user_client ([box_user_obj]): [dobject to make connection to box folder]
            file_info ([tuple]): [containing file meta data like  id,filename,foldername]
        Returns:
            [tuple(str,dict)]: [return the file path wherefile is saved on the disk and dict containing file meta data]
        """
        file_id ,file_name,folder_name = file_info
        file_meta_data={}
        try:
            file_info = user_client.file(file_id).get()
            files_meta_data =file_info.response_object
            
            file_meta_data["file_id"]=file_id
            file_meta_data["file_name"]=file_name
            file_meta_data["folder_name"]=folder_name
            file_meta_data["created_at"]=files_meta_data["content_created_at"]
            file_meta_data["file_name"]=file_info.name


            # print(f'File "{file_info.name}" has a size of {file_info.size} bytes')
            # file_name = file_info.name
        except Exception as e:
            print(e)
            raise e
       
        try:
            
            if file_name[-4:] == "xlsx":
                excel_file_path = BASE_PATH+r"\files\excel\{}".format(file_name)
                with open(excel_file_path, 'wb') as file:
                    user_client.file(file_id).download_to(file)

                return excel_file_path,file_meta_data
        except Exception as e:
            print(e)
            raise e
        
        try:
            # print("inside pdf")

            if file_name[-3:] == "pdf":
                pdf_file_path = BASE_PATH+r"\files\pdf\{}".format(file_name)
               
                with open(pdf_file_path, 'wb') as file:
                    user_client.file(file_id).download_to(file)

                return pdf_file_path,file_meta_data
        except Exception as e:
            print(e)
            raise e

    def get_all_files_in_root(self,user_client):
        """[ all the content in the root folder]
        Args:
            user_client ([bx_obj]): [box object to make connection ]
        
        Returns:
            [list]: [all files and folder in the root folder]
        """
        try:
            items = user_client.folder(folder_id='0').get_items()
            for item in items:
                print(f'{item.type.capitalize()} {item.id} is named "{item.name}"')
            return items
        except Exception as e:
            print(e)
            raise e
        
        
    def box_file_in_data_range_legacy(self,user_client,file_id,start,end):
        file = user_client.file(file_id).get()
        #2022-10-13 23:05:25

        created_at = file.response_object["created_at"].split("T")
        created_at[1] = created_at[1][:8]
        created_at = " ".join(created_at)
        

        
        TODAY_CHECK = datetime.datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
        
        start = datetime.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")

       

        if start >= TODAY_CHECK < end:
            return True
        else:
            return False
        
        
    def box_read_folder_content_legacy(self,user_client,folder_id,start,end):
    

        """[read all content of the folder and return the pdf andd excel files]
        Returns:
            [dict]: [file meta data of both pdf and excel]
        """
       
        folder = user_client.folder(folder_id=folder_id).get()
        folder_name = folder.name
        file_ids={"pdf":[],"xlsx":[]}
        try:
            items = user_client.folder(folder_id=folder_id).get_items()
            for item in items:
                
                # print(f'{item.type.capitalize()} {item.id} is named "{item.name}"')
                if item.name[-3:] == "pdf":
                    print("inside pdf")
                    if self.box_file_in_data_range_legacy(user_client,item.id,start,end):
                        file_ids["pdf"].append((item.id,item.name,folder_name)) 
                if item.name[-4:] == "xlsx":
                    
                    print("indies excel")
                    if self.box_file_in_data_range(user_client,item.id,start,end):
                        file_ids["xlsx"].append((item.id,item.name,folder_name))

            return file_ids
        except Exception as e:
            print(e)
            raise e


print("start")
obj = BOX_UTILS(start_date="2022-10-15",end_date="2022-10-17")
print(obj)

auth_obj = obj.box_auth()

# #excel_file_path,file_meta_data
# file_path ,file_meta_data = obj.download_file(auth_obj,('1039949940900', '132B7467P001-20180719MW report.xlsx', 'test-excel-1'))
# print(file_path)

# folder_data = obj.box_read_folder_content(auth_obj,'177294206571')
# print(folder_data)

# {'pdf': [], 'xlsx': [('1039949940900', '132B7467P001-20180719MW report.xlsx', 'test-excel-1')]}
# for each_flder in config.folder_ids_list:
#     folder_data = obj.box_read_folder_content(auth_obj,each_flder)
#     print(folder_data)


# 1039949940900
# file = auth_obj.file('1039949940900').get()
# print(type(file.response_object))
# # print(file.response_object.keys())
# print(file.response_object["created_at"])
# # print(file.response_object["content_created_at"])

# created_at = file.response_object["created_at"].split("T")
# print(created_at,"ccc")
# created_at[1] = created_at[1][:8]
# # created_at = " ".join(created_at[0],created_at[1][:8])
# print(created_at)
# created_at = " ".join(created_at)
# print(created_at)
# # print(json.dumps(file.response_object))

# # # obj.get_all_files_in_root(auth_obj)

# import datetime
# TODAY_CHECK = datetime.datetime.strptime("2022-10-11 00:00:01", "%Y-%m-%d %H:%M:%S")
# print(TODAY_CHECK)
# # start = datetime.datetime.strptime("2022-10-14", "%Y-%m-%d")
# start = datetime.datetime.strptime("2022-10-11 00:00:00", "%Y-%m-%d %H:%M:%S")

# if TODAY_CHECK > start:
#     print("ok")
# else:
#     print("no")





