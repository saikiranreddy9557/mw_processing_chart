import boto3
import json
import os
from decimal import Decimal


from distutils.command.upload import upload
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))


# print(BASE_DIR)

sys.path.append(BASE_DIR)
# sys.path.append( '.' )
# from config import acces_key,secret_key,region,table_name
import config
# print(config.table_name)





class AWS_Dynamo_Client:
    def __init__(self,  access_key_id=None, secret_access_key=None,region_name = None,table_name =config.table_name):
        """
        
        
        :param acces_key_id: access_key_id
        :param secret_access_key :  secret_access_key
        :param region_name : region name of aws 
        :param table_name : table_name name of aws dynamodb
        """
        try:
            if access_key_id:
                self.dynamo_client = boto3.client('dynamodb', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=region_name,verify =False )

                self.dynamodb_session = boto3.resource('dynamodb',aws_access_key_id=access_key_id,aws_secret_access_key= secret_access_key,region_name=region_name,verify = False)
                self.table =config.table_name
                self.s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=region_name,verify =False )

            else:
                self.dynamo_client = boto3.client('dynamodb')

                self.dynamodb_session = boto3.resource('dynamodb')
                
                self.table =table_name
                self.s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=region_name,verify =False )
        except Exception as e:
            print(e)
            raise e
                




    def list_all_tables(self):
        """list all the tables in the dynamodb cluster
        Returns:
            [list]: [list of all tables]
        """

        try:
            tables = list(self.dynamodb_session.tables.all())
            return tables
            #print(tables)
        except Exception as e:
            print(e)
            raise e

    def push_data(self,data):
        """[performing ucreate operation into dynamodb table]
        Args:
            data ([dict]): [file data in dict format file(pdf/excel)]
        Returns:
            [BOOL]: [true if success or false]
        """
        try:     
            table = self.dynamodb_session.Table(self.table)
        
            data = json.loads(json.dumps(data), parse_float=Decimal)
            response = table.put_item(Item=data)

            return True
        except Exception as e:
            print(e)
            raise e
                 

    def upload_to_aws_s3(self,local_file_path, bucket_name, file_name,execution_id =None):
        

        try:
            path = "sample3/"+execution_id+"/"+file_name
            v = self.s3_client.upload_file(local_file_path, bucket_name, path)
            print("vv",v)
            print("Upload Successful")
            return True
        except Exception as e:
            print(e)
            print("The file was not found")
            return False
        
# file_path = r"C:\Users\sensai\Desktop\mw_processing_chart\files\pdf\MW --423896001-1   7F High Output R0.pdf"
# bucket_name = "sample--bucket--doc"
# s3_file_name = "helm_upload"
# uploaded = upload_to_(file_path, bucket_name, s3_file_name)
# print("sss",uploaded)

# acces_key = config.access_key
# secret_key =config.secret_key
# print(acces_key,secret_key)
# obj =AWS_Dynamo_Client(access_key_id=config.access_key,secret_access_key=config.secret_key,region_name=config.region)
# # obj.push_data
# # obj.dynamo_client
# # ddb_exceptions = obj.dynamo_client.exceptions

# obj.push_data("sa")
# print(ddb_exceptions.messsage)


        
# file_path = r"C:\Users\sensai\Desktop\mw_processing_chart\files\pdf\MW --423896001-1   7F High Output R0.pdf"
# bucket_name = "sample--bucket--doc"
# s3_file_name = "sample3/sample2.pdf"
# uploaded =obj.upload_to_aws_s3(file_path, bucket_name, s3_file_name)
# print("sss",uploaded)





