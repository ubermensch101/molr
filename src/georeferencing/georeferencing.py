from utils import *
from config import *
from scripts import *
import json
from src.data_validation.scripts.analysis.analyse_gcps import *

def georeferencer(village, gcp_label_toggle):
    config = Config()
    if village != "":
        config.setup_details['setup']['village'] = village
    if gcp_label_toggle != "":
        config.setup_details['georef']['gcp_label_toggle'] = gcp_label_toggle
    pgconn = PGConn(config)
    
    return Georeferencer(config,pgconn)

class Georeferencer:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.survey_jitter =  self.config.setup_details['data']['survey_jitter_table']
        self.schema_name = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.survey_georeferenced = self.config.setup_details['data']['survey_georeferenced_table']
        self.gcp_report = self.config.setup_details['georef']['gcp_report']
        self.final_gcps_used = []
        self.final_output = [""]


    def jitter(self):
        jitrun = Georeference_without_gcps(self.config,self.psql_conn)
        jitrun.run()
    
    


    def georef_using_gcps(self):
        tri = Trijunctions(self.config, self.psql_conn)
        tri.run()
        gcpmap = GCP_map(self.config,self.psql_conn)
        gcpmap.run()
        gcps_used_jitter = []
        gcps_used_polynomial_1 = []
        gcps_used_polynomial_2 = []
        gcps_used_projective = []
        gcps_used_spline = []
        poly1 = Polynomial(self.config, self.psql_conn)
        poly2 = Polynomial(self.config, self.psql_conn)
        print("\n-----Georeferencing Using Polynomial 1-----")
        deg_1 , gcps_used_polynomial_1 = poly1.run(1)
        print("\n-----Georeferencing Using Polynomial 2-----")
        deg_2 , gcps_used_polynomial_2 = poly2.run(2)
        print("\n-----Georeferencing Using Projective-----")
        proj = 0
        try:
            projective = Projective(self.config, self.psql_conn)
            proj, gcps_used_projective = projective.run()
        except:
            proj = math.inf
        print("\n-----Georeferencing Using Spline-----")
        spline = 0

        spl = Spline(self.config, self.psql_conn)
        spline, gcps_used_spline = spl.run()
        
            
        
        xs_area_jitter = excess_area_at_boundary([0, 1, 1, 0, 0], self.psql_conn, self.schema_name,  self.survey_jitter, self.farmplots)
        
        excess_areas = [
            [xs_area_jitter, self.survey_jitter, "Jitter", gcps_used_jitter],
            [deg_1, poly1.output, "Polynomial 1", gcps_used_polynomial_1],
            [deg_2, poly2.output, "Polynomial 2", gcps_used_polynomial_2],
            [proj, projective.survey_projective, "Projective", gcps_used_projective],
            [spline, spl.survey_spline, "Spline", gcps_used_spline]
        ]

        excess_areas = list(filter(lambda x: x[0] != math.inf and x[0] != 0, excess_areas))

        excess_area_percentage = []
        for excess_list in excess_areas:
            excess_area_ratio = excess_list[0] / get_farmplot_areas_at_boundary(self.psql_conn, self.schema_name, excess_list[1], self.farmplots)
            excess_area_percentage.append([excess_list[0], excess_list[1], excess_list[2], excess_list[3], excess_area_ratio])
        
        excess_area_percentage.sort(key=lambda x: x[4])
        if len(excess_area_percentage) == 0:
            print("No georeferencing possible using gcps")
            return
        
        self.final_gcps_used.extend(excess_area_percentage[0][3])
        self.final_output[0] = excess_area_percentage[0][2]
        
        
        copy_table(self.psql_conn,self.schema_name +"." + excess_area_percentage[0][1], self.schema_name +"." + self.survey_georeferenced)
        validate_geom(self.psql_conn, self.schema_name, self.survey_georeferenced)

        print("Final Georeferenced Output Selected :-", excess_area_percentage[0][2],
     
         "with excess area percent", 100*excess_area_percentage[0][0]/get_farmplot_areas_at_boundary(self.psql_conn, self.schema_name, self.survey_georeferenced, self.farmplots))

        
    
    def report(self):
        flag = check_column_exists(self.psql_conn, self.schema_name, self.gcp_report, "Parseable/Non_Parseable")
        if flag==False:
            analyse_gcps(self.config, self.psql_conn )
        gcp_trijunction_match(self.config, self.psql_conn)
    
    def run(self):
        self.jitter()
        fix_gaps(self.psql_conn, self.schema_name, self.survey_jitter)
        self.georef_using_gcps()
        self.report()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village",
                        required=False, default="")
    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    argument = parser.parse_args()
    gcp_toggle = argument.gcp_label_toggle
    village = argument.village
    georef = georeferencer(village , gcp_toggle)
    georef.run()
    