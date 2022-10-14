import numpy as np
import pandas as pd
import json
import tabula
import pathlib


# excel_file_path,folder_name,file_name,created_by,created_at
def extract_data_from_excel(excel_file_path,folder_name= None,file_name=None,created_at =None,created_by = None ):
  """[extract data from the excel file ]
  Args:
      excel_file_path ([str]): [file path to excel]
      folder_name ([str], optional): [folder name that is also vendor name]. Defaults to None.
      file_name ([file name], optional): [file name]. Defaults to None.
      created_at ([str], optional): [time stamp when teh file is created]. Defaults to None.
      created_by ([type], optional): [description]. Defaults to None.
  Returns:
      [dict]: [all the file data ]
  """

  try:
    
      #load excel file
      xl = pd.ExcelFile(excel_file_path)
      
      file_sheet_names = xl.sheet_names
      #getting sheets name list
      
      #readind and convertinf each sheet to a dataframe
      t1 = pd.read_excel(excel_file_path, sheet_name=file_sheet_names[0], index_col=0)
      t2 = pd.read_excel(excel_file_path, sheet_name=file_sheet_names[1], index_col=0)
      
      #replace nan to None
      t1 = t1.replace(np.nan, 'None', regex=True)
      t2 = t2.replace(np.nan, 'None', regex=True)

      #fetching additonal value from the first sheet of excel
      stage = t1.loc['级数（Stage）：'].values[0]
    
      po_num = t1.loc['PO  Number:'].values[0]
    
      blade_pn = t1.loc['部套号（Drawing Number）：'].values[0]
      compressor_pn = t1.loc['台份号（Product Number）：'].values[0]
      
      #renaming columns in sheet two columns in dataframe
      t2.rename(columns = {'叶片钢印号[Steel Seal No.]':'blade sn'}, inplace = True)
      rename_dict = {'称重编号[Weight No.]':'Weight No','叶片类型[Blade Type]':'Blade Type','设计重量(克)[Design Weight](g)':'Design Weight','叶根重量(克)[Blade Root Weight]（g)':'Blade Root Weight','叶冠重量(克)[Blade Leaf Weight]（g)':'Blade Leaf Weight','实测重量(克)[Actual Weight]（g)':'Actual Weight','实测力矩(克·英寸)[Actual Moment](g·inch)':'Actual Moment'}
      t2.rename(columns = rename_dict , inplace = True)
    
      #creating new dict with first sheet values
      new_dict = {"chart file name": file_name,"vendor name" : folder_name, "chart date " : created_at,"frame stage ": stage,"blade pn":blade_pn,"compressor pn": compressor_pn,"PO Number":po_num}   
      


      #convert df to dict
      t2_dict = t2.to_dict('list')

      #merge two dict
      new_dict.update(t2_dict)


      return new_dict,excel_file_path
  except Exception as e:
    print(e)
    print("file is not in desierd format")
    return False




# pdf_file_path,folder_name,file_name,created_by,created_at
def extract_data_from_pdf(pdf_file_path,folder_name=None,file_name=None,created_at=None,created_by=None):
   
  """[extracts data from the pdf files]
  Returns:
      [dict]: [all the file data]
  """

  try:
      #read and converting pdf to dataframe
      pdf = tabula.read_pdf(pdf_file_path, pages='all')[0]

      #replace nan columns to None
      pdf = pdf.replace(np.nan, 'None', regex=True)


      
      blade_pn = pdf.iloc[1][1]
      frame_stage = pdf.columns[1]
      compressor_pn = pdf.iloc[0][1]
      
      po_num =  pdf.columns[5].split("\r")[0]
      print(po_num)
      try:
        int(po_num)
      except :
        po_num = None

      t2 = pdf[5:].copy()
      
      #rename of columns
      rename_dict = {'GE叶片名称:\rStages':'Position Number','7F HIFLOW R0':'Serial Number','组别号:\rWTB Group NO.':'MOMENT','P1':'TOTAL WT','旋向\rDIRETION OF\rROTATION':'REACTION','CW':'Part Number'}
      t2.rename(columns = rename_dict , inplace = True)

      #selecting only required columns
      df_new = t2.iloc[:, [0,1,2,3,4,5]]

      #convert dataframe into dict
      t2_dict = df_new.to_dict('list')

      # folder_name="sampl-vendor name",file_name="file name",created_by="2022/2/2"
      new_dict = {"chart file name": file_name,"vendor name" : folder_name, "chart date " : created_by,"frame stage ": frame_stage,"blade pn":blade_pn,"compressor pn": compressor_pn,"PO Number":po_num}


      #merge two dict
      new_dict.update(t2_dict)
      return new_dict,pdf_file_path 
  except Exception as e:
    print(e)
    print("file is not in desierd format")
    return False


# result = extract_data_from_pdf("MW---423839925-2--7F HIFLOW R0  --1.pdf",folder_name="sampl-vendor name",file_name="file name",created_by="2022/2/2",created_at=None)
# print(result)



def delete_file(file_path):
  """[delete file ]
  Args:
      file_path ([str]): [file path]
  Returns:
      [Bool]: [True or false]
  """
  try:
    file = pathlib.Path(file_path)
    # Calling the unlink method on the path
    file.unlink()
    return True
  except Exception as e:
    print(e)
    print("not able to  delete file on disk")
    return False

    
