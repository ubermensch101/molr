from config import *
from utils import *

def analyse_survey_plots():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Analyse_Survey_Plots(config,conn)

class Analyse_Survey_Plots:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        pass

    def analyse(self):
        pass

    def run(self):
        pass
    
if __name__=="__main__":
    asp = analyse_survey_plots()
    asp.run()