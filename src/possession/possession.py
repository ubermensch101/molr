from utils import *
from config import *
import json

def possession():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Possession(config,conn)

class Possession:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def cut_farms(self):
        pass
    
    def run(self):
        pass
    
if __name__=="__main__":
    pos = possession()
    pos.run()
    