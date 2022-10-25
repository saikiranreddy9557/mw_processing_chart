import os
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),))
print(BASE_DIR)
from processing.modules.dynamo_utils import AWS_Dynamo_Client
import json
f = open(r"C:\Users\sensai\Desktop\mw_processing_chart\dynamo_data_MW --423896001-1   7F High Output R0_pdf.json")
  
# returns JSON object as 
# a dictionary
data = json.load(f)
print(type(data))

dynamo_obj = AWS_Dynamo_Client()

dynamo_obj.push_data(data)
  
# Iterating through the json
# # list
# for i in data['emp_details']:
#     print(i)
  
# # Closing file

f.close()
