from config import *
from utils import *

def data_correcter():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Data_Correcter(config,conn)

class Data_Correcter:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def correct(self):
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    datacorrecter = data_correcter()
    datacorrecter.run()