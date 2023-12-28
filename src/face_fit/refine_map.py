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
        self.village = config.setup_details['setup']['village']
        self.srid = config.setup_details['setup']['srid']
        self.shifted_nodes = config.setup_details['fbfs']['shifted_nodes_table']
        
    def midline_jitter(self):
        pass
    
    def spline_jitter(self):
        pass
    
    def local_jitter(self):
        pass
    
    def fix_gcps(self):
        gcp_map_creator = Fix_GCP(self.config, self.psql_conn)
        gcp_map_creator.run()
    
    def facefit_snap(self):
        pass
    
    def setup_fbfs(self):
        setup = Setup_Facefit(self.config,self.psql_conn)
        setup.run()
    
    def run(self):
        self.setup_fbfs()
        create_nodes_table(self.psql_conn, self.village+"."+self.shifted_nodes, self.srid)
        self.fix_gcps()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    fbfs = face_fit(village)
    fbfs.run()
    