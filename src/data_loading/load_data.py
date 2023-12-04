from config import *
from utils import *

def dataloading():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return DataLoading(config,conn)

class DataLoading:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def run(self):
        # Run .sh files located in scripts
        # The .sh files should print what is missing
        pass

if __name__=="__main__":
    dl = dataloading()
    dl.run()