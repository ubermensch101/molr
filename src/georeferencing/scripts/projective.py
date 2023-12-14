import numpy as np
import cv2
import pandas as pd
from itertools import combinations
from .georef_utils import *
from utils import *
import math
from config import *
import argparse

def projective():
    config = Config()
    pgconn = PGConn(config)
    return Projective(config,pgconn)

class Projective:

    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.schema_name = self.config.setup_details['setup']['village']
        self.temp_georeferencing_schema = self.schema_name+ self.config.setup_details['georef']['temp_georeferencing_schema']
        self.gcp = self.config.setup_details['data']['gcp_table']
        self.survey_processed = self.config.setup_details['data']['survey_processed']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.gcp_map = self.config.setup_deatils['georef']['gcp_map']
        self.gcp_label_toggle = self.config.setup_details['georef']['gcp_label_toggle']
        self.survey_projective = self.config.setup_details['georef']['projective']
        self.survey_jitter = self.config.setup_details['data']['survey_jitter_table']
        self.survey_shifted = self.config.setup_details['georef']['survey_shifted']


    def survey_jitter_projective(self, input, gcpmap, output):
        input_table = self.schema_name + "." + input
        gcp_map = self.schema_name + "." + gcpmap
        original_output_name = self.schema_name + "." + output
        output_table = self.temp_georeferencing_schema+"."+output
        sql = f'''
            select st_x(geom),st_y(geom) from {gcp_map}
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            source_points_all = curr.fetchall()
        sql = f"select st_x(gcp_geom),st_y(gcp_geom) from {gcp_map}"
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            target_points_all = curr.fetchall()
        if len(source_points_all) < 4:
            print(f"Cannot perform projective with {len(source_points)} GCPs")
            exit

        sql = f'''
            select gid, (st_dumppoints(geom)).path, 
            st_x((st_dumppoints(geom)).geom), st_y((st_dumppoints(geom)).geom)
            from {input_table}
            order by gid;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            points_with_path = curr.fetchall()
            points_with_path = [(i[0], (i[1], (i[2], i[3])))
                                for i in points_with_path]

        points = np.array([i[1][1]
                          for i in points_with_path], dtype=np.float64)

        results = []
        count = 0
        source_points = source_points_all
        target_points = target_points_all
        source_points = np.array(source_points, dtype=np.float32)
        target_points = np.array(target_points, dtype=np.float32)

        TransformationMatrix, _ = cv2.findHomography(source_points, target_points)

        transformed_points = cv2.perspectiveTransform(points.reshape(-1, 1, 2), TransformationMatrix)
        transformed_points = [(i[0], (i[1][0], (j[0][0], j[0][1])))
                              for i, j in zip(points_with_path, transformed_points)]
        df = pd.DataFrame(transformed_points, columns=["gid", "path_point"])
        recreate_table( self.psql_conn, self.schema_name, input, self.temp_georeferencing_schema, output+f"_{count}", df)
        excess_area = excess_area_without_parameters(self.psql_conn, self.temp_georeferencing_schema, output+f"_{count}", self.farmplots, self.schema_name)
        distortion = get_distortion(self.psql_conn, self.temp_georeferencing_schema, output+f"_{count}", self.survey_processed, self.schema_name)
        results.append([count, df, excess_area, distortion, source_points, target_points])

        used_gcps = max(len(source_points)-2, 4)
        for comb in list(combinations(list(range(len(source_points))), used_gcps)):
            count += 1
            source_points = [source_points_all[i] for i in comb]
            target_points = [target_points_all[i] for i in comb]
            source_points = np.array(source_points, dtype=np.float32)
            target_points = np.array(target_points, dtype=np.float32)

            TransformationMatrix, _ = cv2.findHomography(source_points, target_points)

            transformed_points = cv2.perspectiveTransform(points.reshape(-1, 1, 2), TransformationMatrix)
            transformed_points = [(i[0], (i[1][0], (j[0][0], j[0][1])))
                                  for i, j in zip(points_with_path, transformed_points)]
            df = pd.DataFrame(transformed_points, columns=["gid", "path_point"])
            recreate_table(self.psql_conn, self.schema_name, input, self.temp_georeferencing_schema, output+f"_{count}", df)
            excess_area = excess_area_without_parameters(self.psql_conn, self.temp_georeferencing_schema, output+f"_{count}", self.farmplots, self.schema_name)
            distortion = get_distortion(self.psql_conn, self.temp_georeferencing_schema, output+f"_{count}", self.survey_processed, self.schema_name)
            results.append([count, df, excess_area, distortion,source_points, target_points])

        for i in results:
            g = []
            for j in i[5]:

                sql = '''
                    select gid from {gcp}
                    where st_intersects(st_point({x},{y},32643),st_buffer(geom,0.5));
                '''.format(
                    gcp=self.gcp,
                    x=j[0],
                    y=j[1]
                )
                with self.psql_conn.connection().cursor() as curr:
                    curr.execute(sql)
                    gcp_gid = curr.fetchall()
                    g.append(gcp_gid[0][0])
            print("Index :-", i[0], "GCPs :-", g,
                  "Excess_Area :-", i[2], "Distortion :-", i[3])

        results = list(filter(lambda x: x[3] < 5, results))
        results.sort(key=lambda x: x[2])
        if len(results) == 0:
            return math.inf
        i = results[0]

        recreate_table(self.psql_conn, self.schema_name, input, self.schema_name, original_output_name, i[1])

        g = []
        for j in i[5]:

            sql = '''
                select gid from {gcp}
                where st_intersects(st_point({x},{y},32643),st_buffer(geom,0.5));
            '''.format(
                gcp=self.gcp,
                x=j[0],
                y=j[1]
            )
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
                gcp_gid = curr.fetchall()
                g.append(gcp_gid[0][0])

        print("Best Output :-")
        print("Index :-", i[0], "GCPs :-", g,
              "Excess_Area :-", i[2], "Distortion :-", i[3])

        return i[2],g
    
    def run(self):
        if self.gcp_label_toggle == "True":
            used_map = self.survey_shifted
        elif self.gcp_label_toggle == "False":
            used_map = self.survey_jitter
        output = self.survey_projective
        return self.survey_jitter_projective(used_map, self.gcp_map, output)
        
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")
    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    argument = parser.parse_args()
    gcp_label_toggle = argument.gcp_label_toggle
    proj = projective()
    if gcp_label_toggle != "":
        proj.gcp_label_toggle = gcp_label_toggle
    excess_area, gcps_used = proj.run()
