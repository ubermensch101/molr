from utils import *
from config import *
import json
import argparse

def trijunctions(gcp_label_toggle):
    config = Config()
    if village != "":
        config.setup_details['setup']['village'] = village
    if gcp_label_toggle != "":
        config.setup_details['georef']['gcp_label_toggle'] = gcp_label_toggle
    pgconn = PGConn(config)
    obj = Trijunctions_and_Map(config,pgconn)
    return obj

class Trijunctions_and_Map:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.schema_name = self.config.setup_details['setup']['village']
        self.survey_jitter = self.config.setup_details['data']['survey_jitter_table']
        self.survey_shifted = self.config.setup_details['georef']['survey_shifted']
        self.survey_shifted_vertices = self.config.setup_details['georef']['survey_shifted_vertices']
        self.survey_jitter_vertices = self.config.setup_details['georef']['survey_jitter_vertices']
        self.option = self.config.setup_details['georef']['gcp_label_toggle']
        self.survey_label_column = self.config.setup_details['georef']['survey_label_column'] 
        self.gcp_map = self.config.setup_details['georef']['gcp_map']
        self.gcp = self.config.setup_details['data']['gcp_table']

    def find_tri_junctions(self, input, topo_name, output):
        create_topo(self.psql_conn, self.schema_name, topo_name, input)
        get_corner_nodes(self.psql_conn, topo_name, self.schema_name, output, only_trijunctions=True)
        create_node_labels(self.psql_conn, self.schema_name, input, output)
    

    def run(self):
        if self.option=="True":
            used_map = self.survey_shifted
            used_map_vertices = self.survey_shifted_vertices
            toggle = True
        elif self.option == "False" :
            used_map = self.survey_jitter
            used_map_vertices = self.survey_jitter_vertices
            toggle = False
        topo_name = self.schema_name+"_"+used_map+"_topo"
        self.find_tri_junctions(used_map, topo_name, used_map_vertices)
        create_gcp_map(self.psql_conn, self.schema_name, used_map_vertices, self.gcp, self.gcp_map, use_labels = toggle)
        add_gcp_label(self.psql_conn, self.schema_name, used_map_vertices, self.gcp, self.gcp_map )




if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")
    parser.add_argument("-v", "--village", help="Village",
                        required=False, default="")
    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    argument = parser.parse_args()
    village = argument.village
    gcp_label_toggle = argument.gcp_label_toggle
    trijun = trijunctions(village, gcp_label_toggle)
    trijun.run()
    
