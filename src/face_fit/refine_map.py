from utils import *
from config import *
from scripts import *
import argparse

def face_fit(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Face_Fit(config,pgconn)

class Face_Fit:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.gcp = config.setup_details['data']['gcp_table']
        self.village = config.setup_details['setup']['village']
        self.srid = config.setup_details['setup']['srid']
        self.farmplots = config.setup_details['data']['farmplots_table']
        
        self.narrow_faces_method = config.setup_details['fbfs']['narrow_face_method']
        self.face_fit_method = config.setup_details['fbfs']['face_fit_method']
        
        self.shifted_faces = config.setup_details['fbfs']['shifted_faces_table']
    
    def fix_gcps(self):
        gcp_map_creator = Fix_GCP(self.config, self.psql_conn)
        gcp_map_creator.run()
    
    def fix_narrow_faces(self):
        if self.narrow_faces_method=="jitter":
            nar_jit = Narrow_Jitter(self.config, self.psql_conn)
            nar_jit.run()
        # elif self.narrow_faces_method=="one-off":
        #     one_off = Narrow_One_Off(self.config, self.psql_conn)
        #     one_off.run()
    
    def fix_original_faces(self):
        # if self.face_fit_method=="snap":
        #     snap_fit = Snap_Fit(self.config, self.psql_conn)
        #     snap_fit.run()
        # elif self.face_fit_method=="jitter_spline":
        #     jit_spline = Jitter_Spline(self.config, self.psql_conn)
        #     jit_spline.run()
        # elif self.face_fit_method=="jitter_midline":
        #     jit_midline = Jitter_Midline(self.config, self.psql_conn)
        #     jit_midline.run()
        pass
    
    def setup_fbfs(self):
        setup = Setup_Facefit(self.config,self.psql_conn)
        setup.run()
        
    def validate(self):
        add_varp(self.psql_conn, self.village, self.shifted_faces, 'varp')
        add_farm_intersection(self.psql_conn, self.village, self.shifted_faces, self.farmplots, 'farm_intersection')
        add_farm_rating(self.psql_conn, self.village, self.shifted_faces, self.farmplots, 'farm_rating')
        add_shape_index(self.psql_conn, self.village, self.shifted_faces, 'shape_index')
        
    def run(self):
        self.setup_fbfs()
        self.fix_gcps()
        self.fix_narrow_faces()
        self.fix_original_faces()
        self.validate()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    fbfs = face_fit(village)
    fbfs.run()
    