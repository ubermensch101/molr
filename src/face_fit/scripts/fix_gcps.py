from config import *
from utils import *
import argparse

def fix_gcp(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Fix_GCP(config,pgconn)

class Fix_GCP:
    def __init__(self, config: Config, psql_conn: PGConn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.gcp = self.config.setup_details['data']['gcp_table']
        
        self.inp = self.config.setup_details['fbfs']['input_table']
        self.topo = self.village + self.config.setup_details['fbfs']['input_topo_suffix']
        
        self.nodes = self.config.setup_details['fbfs']['corner_nodes']
        self.nodes_label = self.config.setup_details['fbfs']['corner_nodes_label_column']
        self.nodes_buf_thresh = self.config.setup_details['fbfs']['corner_nodes_label_buf_thresh']
        self.gcp_label = self.config.setup_details['val']['gcp_label']
        self.vb_label = self.config.setup_details['val']['vb_gcp_label']
        self.gcp_labeling_convention = self.config.setup_details['val']['gcp_label_convention']
        self.gcp_map = self.config.setup_details['fbfs']['gcp_map_table']
        self.label_delim = self.config.setup_details['val']['label_delimiter']

        self.angle_thresh = self.config.setup_details['fbfs']['corner_nodes_angle_thresh']
        self.survey_no = self.config.setup_details['val']['survey_no_label']
        self.srid = self.config.setup_details['setup']['srid']
        
        self.covered_nodes = config.setup_details['fbfs']['covered_nodes_table']
        self.covered_edges = config.setup_details['fbfs']['covered_edges_table']
        self.covered_faces = config.setup_details['fbfs']['covered_faces_table']
        self.face_node_map = config.setup_details['fbfs']['face_node_map_table']

        if self.village == "":
            print("ERROR")
            exit()             
    
    def run(self):
        get_corner_nodes(self.psql_conn, self.topo, self.village, self.nodes, self.angle_thresh)
        create_node_labels(self.psql_conn, self.village, self.inp, self.nodes, self.survey_no, 
                           self.nodes_label, self.nodes_buf_thresh, self.vb_label, self.gcp_labeling_convention)
        create_gcp_map(self.psql_conn, self.village, self.nodes, self.gcp, self.gcp_map, self.nodes_label,
                       self.gcp_label, use_labels=True, delimiter_regex=self.label_delim)
        commit_nodes(self.psql_conn, self.topo, self.village, self.covered_nodes, self.covered_edges,
                     self.covered_faces, self.gcp_map, 'node_id', 'gcp_geom', self.face_node_map)
        
if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    gcp_fixing = fix_gcp(village)
    gcp_fixing.run()

