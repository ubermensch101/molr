from utils import *
from config import *
from scripts import *
import json
import argparse

def face_fit(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Face_Fit(config,pgconn)

class Face_Fit:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.gcp = config.setup_details['data']['gcp_table']
        
    def midline_jitter(self):
        pass
    
    def spline_jitter(self):
        pass
    
    def local_jitter(self):
        pass
    
    def gcp_map(self):
        gcp_map_creator = Get_GCP_Map(self.config, self.psql_conn)
        gcp_map_creator.run()
    
    def facefit_snap(self):
        pass
    
    def setup_fbfs(self):
        setup = Setup_Facefit(self.config,self.psql_conn)
        setup.run()
    
    def run(self):
        self.setup_fbfs()
        self.gcp_map()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    fbfs = face_fit(village)
    fbfs.run()
    