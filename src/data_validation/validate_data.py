from config import *
from utils import *
from scripts import *
import argparse

def datavalidation(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return DataValidationAndPreparation(config,pgconn)

class DataValidationAndPreparation:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn

    def analyse(self):
        # Prepare analysis summary: what is missing, what is duplicate, non-alphanumeric, etc
        
        analyse_survey_plots(self.config,self.psql_conn)
        analyse_cadastrals(self.config,self.psql_conn)
        analyse_gcps(self.config,self.psql_conn)
        analyse_akarbandh(self.config,self.psql_conn)

    def clean_data(self):
        # wrapper for clean data
        cleaner = Data_Cleaner(self.config,self.psql_conn)
        cleaner.run()

    def prepare_survey_map(self):
        # wrapper for correct data
        survey_map_prep = Survey_Map_Processer(self.config,self.psql_conn)
        survey_map_prep.run()

    def run(self):
        self.clean_data()
        self.analyse()
        self.prepare_survey_map()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    datacleaner = datavalidation(village)
    datacleaner.run()
