from config import *
from utils import *
import argparse

def get_gcp_map(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Get_GCP_Map(config,pgconn)

class Get_GCP_Map:
    def __init__(self, config: Config, psql_conn: PGConn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.gcp = self.config.setup_details['data']['gcp_table']
        
        self.inp = self.config.setup_details['fbfs']['input_table']
        self.ori = self.config.setup_details['fbfs']['original_faces_table']
        self.nar = self.config.setup_details['fbfs']['narrow_faces_table']
        self.topo = self.village + self.config.setup_details['fbfs']['input_topo_suffix']
        self.nar_mid = self.config.setup_details['fbfs']['narrow_midlines_table']
        self.nodes = self.config.setup_details['fbfs']['corner_nodes']
        self.nodes_label = self.config.setup_details['fbfs']['corner_nodes_label_column']
        self.nodes_buf_thresh = self.config.setup_details['fbfs']['corner_nodes_label_buf_thresh']
        self.gcp_label = self.config.setup_details['val']['gcp_label']
        self.vb_label = self.config.setup_details['val']['vb_gcp_label']
        self.gcp_labeling_convention = self.config.setup_details['val']['gcp_label_convention']
        self.gcp_map = self.config.setup_details['fbfs']['gcp_map_table']
        self.label_delim = self.config.setup_details['val']['label_delimiter']

        self.tol = self.config.setup_details['fbfs']['topo_tol']
        self.seg_tol = self.config.setup_details['fbfs']['seg_tol']
        self.seg_length = self.config.setup_details['fbfs']['seg_length']
        self.nar_face_shp_index_thresh = self.config.setup_details['fbfs']['nar_face_shp_index_thresh']
        self.intersection_thresh = self.config.setup_details['fbfs']['survey_no_assignment_intersection_thresh']
        
        self.angle_thresh = self.config.setup_details['fbfs']['corner_nodes_angle_thresh']
        self.survey_no = self.config.setup_details['val']['survey_no_label']

        if self.village == "":
            print("ERROR")
            exit()             
    
    def run(self):
        get_corner_nodes(self.psql_conn, self.topo, self.village, self.nodes, self.angle_thresh)
        create_node_labels(self.psql_conn, self.village, self.inp+"_t", self.nodes, self.survey_no, 
                           self.nodes_label, self.nodes_buf_thresh, self.vb_label, self.gcp_labeling_convention)
        create_gcp_map(self.psql_conn, self.village, self.nodes, self.gcp, self.gcp_map, self.nodes_label,
                       self.gcp_label, use_labels=True, delimiter_regex=self.label_delim)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    midlines = get_gcp_map(village)
    midlines.run()

