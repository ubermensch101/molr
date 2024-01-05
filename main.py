from config import *
from utils import *
import argparse

def main(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Main(config, pgconn)

class Main:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass
        
    def run_data_loading(self):
        pass
    
    def run_data_validation(self):
        pass
    
    def run_farmplot_processing(self):
        pass
    
    def run_georeferencing(self):
        pass
    
    def run_face_fit(self):
        pass
    
    def run_possession(self):
        pass
    
    def run_validation(self):
        pass
    
    def run(self):
        pass

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    main_pipeline = main(village)
    main_pipeline.run()
    
    
    