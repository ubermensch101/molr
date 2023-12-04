from config import *
from utils import *

def datavalidation():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return DataValidation(config,conn)

class DataValidation:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def analyse(self):
        # Prepare analysis summary: what is missing, what is duplicate, non-alphanumeric, etc
        pass

    def clean_data(self):
        # wrapper for clean data
        pass

    def correct_data(self):
        # wrapper for correct data
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    dv = datavalidation()
    dv.run()
