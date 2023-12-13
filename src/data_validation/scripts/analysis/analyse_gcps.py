from config import *
from utils import *
import argparse

def analyse_gcps(config, psql_conn):
    pass

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")
    
    argument = parser.parse_args()
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    analyse_gcps(config, pgconn)
