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
        self.farmplots = self.config.setup_details["data"]["farmplots_table"]
        self.village = self.config.setup_details["setup"]["village"]
        
    def clean(self):
        fpc = Farmplot_Cleaner(self.config, self.psql_conn)
        fpc.run()
    
    def midline(self):
        mid = Midline_Creator(self.config,self.psql_conn)
        mid.run()
    
    def create_topology(self):
        topo = Farmplot_Topo_Creator(self.config, self.psql_conn)
        topo.run()
    
    def valid_farm_nodes(self):
        create_valid_farm_nodes(self.config,self.psql_conn)
        
    def validate_fp(self):
        validate_farmplots(self.config, self.psql_conn)
    
    def run(self):
        copy_table(self.psql_conn, 
                   self.village+'.'+self.farmplots, 
                   self.village+'.'+self.config.setup_details["data"]["original_farmplots_table"])
        self.clean()
        self.midline()
        self.create_topology()
        self.valid_farm_nodes()
        self.validate_fp()       

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    fg = farm_graph(village)
    fg.run()
    