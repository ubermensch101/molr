import json
from config import *
from utils import *

def main():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Main(config,conn)

class Main:
    def __init__(self, config, conn):
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
    main_pipeline = main()
    main_pipeline.run()
    
    
    
    