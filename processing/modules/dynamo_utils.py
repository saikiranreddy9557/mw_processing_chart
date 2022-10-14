import boto3
import json
import os

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

            else:
                self.dynamo_client = boto3.client('dynamodb')

                self.dynamodb_session = boto3.resource('dynamodb')
                
                self.table =table_name
        except Exception as e:
            print(e)
                




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

    def push_data(self,data):
        """[performing ucreate operation into dynamodb table]
        Args:
            data ([dict]): [file data in dict format file(pdf/excel)]
        Returns:
            [BOOL]: [true if success or false]
        """
        try:     
            table = self.dynamodb_session.Table(self.table)
            item={  'chart_data_hash':"ssskskskss",
                    'id': 1,
                    'sample':["assa","@2333",None],
                    'title': 'my-document-title',
                    'content': 'some-content',
                }
            response = table.put_item(Item=item)
            # print(response)

            # print("upload success")

            return True
        except Exception as e:
            print(e)
            return False     
# acces_key = config.access_key
# secret_key =config.secret_key
# print(acces_key,secret_key)
# obj =AWS_Dynamo_Client(access_key_id=config.access_key,secret_access_key=config.secret_key,region_name=config.region)
# # obj.push_data
# # obj.dynamo_client
# # ddb_exceptions = obj.dynamo_client.exceptions

# obj.push_data("sa")
# print(ddb_exceptions.messsage)





