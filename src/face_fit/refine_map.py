from utils import *
from config import *
import json

def face_fit():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Face_Fit(config,conn)

class Face_Fit:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def midline_jitter(self):
        pass
    
    def spline_jitter(self):
        pass
    
    def local_jitter(self):
        pass
    
    def gcp_map(self):
        pass
    
    def facefit_snap(self):
        pass
    
    def run(self):
        pass
    
if __name__=="__main__":
    fbfs = face_fit()
    fbfs.run()
    