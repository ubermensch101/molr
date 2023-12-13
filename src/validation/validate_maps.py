from utils import *
from config import *
from scripts import *
import argparse

def validation(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Validation(config,pgconn)

class Validation:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        
    def report(self):
        # generates the report table in the village schema
        add_report(self.config, self.psql_conn)
    
    def summary(self):
        # add the summary of the village to the summary table (common across all villages)
        add_summary(self.config, self.psql_conn)
    
    def setup_validation(self):
        # add stats to current maps to make validation easier
        sv = Setup_Validate(self.config, self.psql_conn)
        sv.run()
    
    def run(self):
        self.setup_validation()
        self.report()
        self.summary()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    
    validate = validation(village)
    validate.run()
    