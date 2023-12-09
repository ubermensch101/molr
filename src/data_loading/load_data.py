from config import *
from utils import *
from scripts import *
import argparse
import os
import subprocess

def dataloading(path_to_data = "", toggle = ""):
    config = Config()
    
    if path_to_data != "":
        config.setup_details["data"]["path"] = path_to_data
    if toggle != "":
        config.setup_details["data"]["toggle"] = toggle
    
    pgconn = PGConn(config)
    
    return DataLoading(config,pgconn)

class DataLoading:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.path = self.config.setup_details["data"]["path"]
        if self.path == "":
            print("Data path not set")
            exit()
        self.toggle = self.config.setup_details["data"]["toggle"]

    def run(self):
        for (root,dirs,files) in os.walk(self.path, topdown=True):
            
            if root[len(self.path):].count(os.sep)!=self.toggle:
                continue
                
            village = "_".join(os.path.basename(root).split('_')[1:])
            vincode = os.path.basename(root).split('_')[0]
            print("Processing village",village,", vincode",vincode)
            
            create_schema(self.psql_conn.connection(), village, delete_original= False)
                
            for dir in dirs:
                if dir.startswith("09"):
                    pass
                    
                elif dir.startswith("14"):
                    pass
                    
                elif dir.startswith("15"):
                    pass
                    
                elif dir.startswith("16"):
                    load_cadastrals(self.psql_conn, os.path.join(root,dir), village)
                    

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-p", "--path", help="Path to data",
                        required=False, default="")
    parser.add_argument("-t", "--toggle", help="0 for village path, 1 for taluka path, 2 for district path and 3 for state path",
                        required=False, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    toggle = argument.toggle
    
    if path_to_data != "":
        if toggle == "":
            print("path specified but toggle not given")
            exit()
        dl = dataloading(path_to_data, toggle)
    else:
        dl = dataloading()
        
    dl.run()