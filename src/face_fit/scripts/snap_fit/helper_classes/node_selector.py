class Node_Selector:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        
        self.inp = self.config.setup_details['fbfs']['input_table']
        self.ori = self.config.setup_details['fbfs']['original_faces_table']
        self.nar = self.config.setup_details['fbfs']['narrow_faces_table']
        self.topo = self.village + self.config.setup_details['fbfs']['input_topo_suffix']
        self.nar_mid = self.config.setup_details['fbfs']['narrow_midlines_table']

        self.angle_thresh = self.config.setup_details['fbfs']['corner_nodes_angle_thresh']
        self.survey_no = self.config.setup_details['val']['survey_no_label']
        self.srid = self.config.setup_details['setup']['srid']
        
        self.covered_nodes = config.setup_details['fbfs']['covered_nodes_table']
        self.covered_edges = config.setup_details['fbfs']['covered_edges_table']
        self.covered_faces = config.setup_details['fbfs']['covered_faces_table']
        self.face_node_map = config.setup_details['fbfs']['face_node_map_table']

    def get_possible_snaps(self, face_id):
        
        pass
