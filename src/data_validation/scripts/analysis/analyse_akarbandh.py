from config import *
from utils import *

def analyse_akarbandh():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Analyse_Akarbandh(config,conn)

class Analyse_Akarbandh:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def analyse(self):
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    aa = analyse_akarbandh()
    aa.run()
