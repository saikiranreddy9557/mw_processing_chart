import json
    
def bfs_serach_folder(root_folder_id):
    que_folder= [] 
    que_folder.append(root_folder_id)
    
    file_ids = []
    
    while que_folder:
        
        folder_id = que_folder.pop(0)
        
        items = user_client.folder(folder_id= folder_id).get_items()
        for item in items:
            
            if item.type == "folder":
                que_folder.append(item.id)
            if item.type == "file":
                if item.name[-3:] == "pdf":
                    file_ids.append(item.id)
                    id = {"id":item.id}
                    json_object = json.dumps(id)
                    with open("file_ids.json", "a") as outfile:
                        outfile.write(item.id+"\n")
                    print(item.id)
                if item.name[-4:] == "xlsx":
                    file_ids.append(item.id)
                    id = {"id":item.id}
                    json_object = json.dumps(id)
                    with open("file_ids.json", "a") as outfile:
                        outfile.write(item.id+"\n")
                    print(item.id)
                    
    return file_ids


print(bfs_serach_folder("177293289704"))
                
    
