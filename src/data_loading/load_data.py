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
        self.toggle = self.config.setup_details["data"]["toggle"]
        self.toggle = int(self.toggle)
        
    def run(self):
        if self.path == "":
            print("Data path not set")
            return
        if self.toggle == "":
            print("Toggle not given")
            return
        self.toggle = int(self.toggle)
        for (root,dirs,files) in os.walk(self.path, topdown=True):
            
            if root[len(self.path):].count(os.sep)!=self.toggle:
                continue
                
            village = ("_".join(os.path.basename(root).split('_')[1:])).lower()
            village = village.replace(' ','')
            vincode = os.path.basename(root).split('_')[0]
            print("Processing village",village,", vincode",vincode)
            
            self.config.setup_details['setup']['village'] = village
            
            create_schema(self.psql_conn.connection(), village, delete_original= False)
                
            for dir in dirs:
                if dir.startswith("09"):
                    load_survey_plots(self.config,self.psql_conn, os.path.join(root,dir))
                    
                elif dir.startswith("14"):
                    load_gcps(self.config,self.psql_conn, os.path.join(root,dir))
                    
                elif dir.startswith("15"):
                    load_akarbandh(self.config,self.psql_conn, os.path.join(root,dir))
                    
                elif dir.startswith("16"):
                    load_cadastrals(self.config,self.psql_conn, os.path.join(root,dir))

def farmplotloading(path_to_farmplots = ""):
    config = Config()
    
    if path_to_farmplots != "":
        config.setup_details["data"]["farmplots_path"] = path_to_farmplots
    
    pgconn = PGConn(config)
    
    return FarmplotLoading(config,pgconn)

class FarmplotLoading:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.path = self.config.setup_details["data"]["farmplots_path"]
        self.toggle = self.config.setup_details["data"]["toggle"]
        self.toggle = int(self.toggle)
        
    def run(self):
        if self.path == "":
            print("Data path not set")
            return
        table_name = self.config.setup_details["data"]["farmplots_table"]
        for (root,dirs,files) in os.walk(self.path, topdown=True):
            for file in files:
                if file.endswith(".kml"):
                    village = file.split('_')[0].lower()
                    village = village.replace(' ','')
                    print(file,village)
                    file_location = os.path.join(root,file)
                    ogr2ogr_cmd = [
                        'ogr2ogr','-f','PostgreSQL',
                        'PG:dbname=' + self.psql_conn.details["database"] + ' host=' +
                            self.psql_conn.details["host"] + ' user=' + self.psql_conn.details["user"] +
                            ' password=' + self.psql_conn.details["password"],
                        file_location,
                        '-lco', 'OVERWRITE=YES',
                        '-lco', 'GEOMETRY_NAME=geom',
                        '-lco', 'schema=' + village, 
                        '-lco', 'SPATIAL_INDEX=GIST',
                        '-lco', 'FID=gid',
                        '-nlt', 'PROMOTE_TO_MULTI',
                        '-nln', table_name
                    ]
                    subprocess.run(ogr2ogr_cmd)        
        
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-p", "--path", help="Path to data",
                        required=False, default="")
    parser.add_argument("-t", "--toggle", help="0 for village path, 1 for taluka path, 2 for district path and 3 for state path",
                        required=False, default="")
    parser.add_argument("-f", "--farmpath", help="Path to farmplots",
                        required=False, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    toggle = argument.toggle
    path_to_farmplots = argument.farmpath
    
    dl = dataloading(path_to_data, toggle)
    dl.run()
    
    fl = farmplotloading(path_to_farmplots)
    fl.run()