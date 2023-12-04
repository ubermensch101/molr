from utils import *
from config import *
import json

def validation():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Validation(config,conn)

class Validation:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def report(self):
        pass
    
    def summary(self):
        pass
    
    def add_stats_to_maps(self):
        pass
    
    def run(self):
        pass
    
if __name__=="__main__":
    validate = validation()
    validate.run()
    