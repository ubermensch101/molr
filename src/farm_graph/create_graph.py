from utils import *
from config import *
import json

def farm_graph():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Farm_Graph(config,conn)

class Farm_Graph:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def clean(self):
        pass
    
    def midline(self):
        pass
    
    def create_topology(self):
        pass
    
    def run(self):
        pass
    
if __name__=="__main__":
    pos = farm_graph()
    pos.run()
    