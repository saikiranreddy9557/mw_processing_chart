dic_val["bladeData"]= []

# print(dic_val["Serial Number"])

for current_index in range(len(dic_val['Serial Number'])):
    temp_dict = {}
    temp_dict["PositionNumber"]=dic_val["Position Number"][current_index]
    temp_dict["SerialNumber"]=dic_val["Serial Number"][current_index]
    temp_dict["MOMENT"]=dic_val["MOMENT"][current_index]
    temp_dict["TOTAL WT"]=dic_val["TOTAL WT"][current_index]
    temp_dict["REACTION"]=dic_val["REACTION"][current_index]
    temp_dict["PartNumber"]=dic_val["Part Number"][current_index]
    dic_val["bladeData"].append(temp_dict)
