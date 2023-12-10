from utils import *
from config import *
from scripts import *
import argparse

def farm_graph(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Farm_Graph(config,pgconn)

class Farm_Graph:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def load(self):
        pass
        
    def clean(self):
        pass
    
    def midline(self):
        pass
    
    def create_topology(self):
        pass
    
    def run(self):
        pass

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="deolanabk")

    argument = parser.parse_args()
    
    village = argument.village

    fg = farm_graph(village)
    fg.run()
    