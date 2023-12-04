from config import *
from utils import *

def analyse_gcps():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Analyse_Gcps(config,conn)

class Analyse_Gcps:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def analyse(self):
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    ag = analyse_gcps()
    ag.run()