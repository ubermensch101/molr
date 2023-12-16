from utils import *
from config import *
import json
import ast
import argparse

def georeference_withput_gcps(village , gcp_label_toggle):
    config = Config()
    if village != "":
        config.setup_details['setup']['village'] = village
    if gcp_label_toggle != "":
        config.setup_details['georef']['gcp_label_toggle'] = gcp_label_toggle
    pgconn = PGConn(config)
    return Georeference_without_gcps(config,pgconn)


class Georeference_without_gcps:

    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.schema_name = self.config.setup_details['setup']['village']
        self.survey_processed = self.config.setup_details['data']['survey_processed']
        self.cadastrals = self.config.setup_details['data']['cadastrals_table']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.gcp = self.config.setup_details['data']['gcp_table']
        self.survey_jitter = self.config.setup_details['data']['survey_jitter_table']
        self.survey_shifted = self.config.setup_details['georef']['survey_shifted']

    def shift_a_to_b(self, a, b, output):
        output_table= self.schema_name + "." + output
        a = self.schema_name + "." + a
        b = self.schema_name + "." + b
        sql = f'''
            drop table if exists {output_table};
            
            create table {output_table} as table {a};
                
            with delta as (
                select st_x(st_centroid(st_union(p.geom))) - st_x(st_centroid(st_union(q.geom))) as dx,
                st_y(st_centroid(st_union(p.geom))) - st_y(st_centroid(st_union(q.geom))) as dy
                from {b} as p, {a} as q 
            )
            update {output_table} 
            set geom = st_translate(geom,(select dx from delta),(select dy from delta));
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def create_survey_jitter(self, bnd1, bnd2):
        self.shift_a_to_b(self.survey_processed, self.cadastrals, self.survey_shifted)
        survey_scaled_rotated = "survey_scaled_rotated"
        jitter_fit(self.psql_conn, self.schema_name, self.survey_shifted, survey_scaled_rotated, self.cadastrals, 0, bnd1)
        jitter_fit(self.psql_conn, self.schema_name, survey_scaled_rotated, self.survey_jitter, self.farmplots, 1, bnd2 )

    def run(self):
        bnd1 = self.config.setup_details['georef']['bnd1']
        bnd1 = ast.literal_eval(bnd1)
        bnd2 = self.config.setup_details['georef']['bnd2']
        bnd2 = ast.literal_eval(bnd2)
        self.create_survey_jitter(bnd1, bnd2)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village",
                        required=False, default="")
    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    argument = parser.parse_args()
    gcp_toggle = argument.gcp_label_toggle
    village = argument.village
    georef = georeference_withput_gcps(village , gcp_toggle)
    georef.run()