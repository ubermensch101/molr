from utils import *
from config import *
import json

def georeferencer():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    
    return Georeferencer(config,pgconn)

class Georeferencer:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def jitter(self):
        pass
    
    def label_gcps(self):
        pass
    
    def georef_using_gcps(self):
        pass
    
    def report(self):
        pass
    
    def run(self):
        pass
    
if __name__=="__main__":
    georef = georeferencer()
    georef.run()
    