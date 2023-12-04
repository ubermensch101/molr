from config import *
from utils import *

def analyse_cadastrals():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Analyse_Cadastrals(config,conn)

class Analyse_Cadastrals:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def analyse(self):
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    ac = analyse_cadastrals()
    ac.run()