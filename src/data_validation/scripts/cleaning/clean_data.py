from config import *
from utils import *

def data_cleaner():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Data_Cleaner(config,conn)

class Data_Cleaner:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def clean(self):
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    datacleaner = data_cleaner()
    datacleaner.run()