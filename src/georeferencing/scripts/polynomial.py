import numpy as np
from itertools import combinations
import math
from config import *
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from utils import *
from  .georef_utils import *
import argparse

def polynomial(village, gcp_label_toggle):
    config = Config()
    if village != "":
        config.setup_details['setup']['village'] = village
    if gcp_label_toggle != "":
        config.setup_details['georef']['gcp_label_toggle'] = gcp_label_toggle
    pgconn = PGConn(config)
    return Polynomial(config,pgconn)


class Polynomial:
    
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.schema_name = self.config.setup_details['setup']['village']
        self.temp_georeferencing_schema = self.schema_name+ self.config.setup_details['georef']['temp_georeferencing_schema']
        self.gcp = self.config.setup_details['data']['gcp_table']
        self.survey_processed = self.config.setup_details['data']['survey_processed']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.gcp_map = self.config.setup_details['georef']['gcp_map']
        self.gcp_label_toggle = self.config.setup_details['georef']['gcp_label_toggle']
        self.survey_jitter_degree = self.config.setup_details['georef']['polynomial']
        self.survey_jitter = self.config.setup_details['data']['survey_jitter_table']
        self.survey_shifted = self.config.setup_details['georef']['survey_shifted']

    def survey_jitter_higher_order(self, input, gcpmap, output, degree):
        gcp_map = self.schema_name + "." + gcpmap
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

        results = []
        count = 0
        source_points = source_points_all
        target_points = target_points_all
        source_points = np.array(source_points, dtype=np.float32)
        target_points = np.array(target_points, dtype=np.float32)

        transformed_df = self.fitting_source_target_points(degree, source_points, target_points, input, output+f"_{count}")
        results.append([count,] + transformed_df + [source_points, target_points])

        used_gcps = int(max(len(source_points)-2, (degree+2)*(degree+1)/2))
        for comb in list(combinations(list(range(len(source_points))), used_gcps)):
            count += 1
            source_points = [source_points_all[i] for i in comb]
            target_points = [target_points_all[i] for i in comb]
            source_points = np.array(source_points, dtype=np.float32)
            target_points = np.array(target_points, dtype=np.float32)

            transformed_df = self.fitting_source_target_points(degree, source_points, target_points, input, output+f"_{count}")
            results.append([count,] + transformed_df + [source_points, target_points])

        for i in results:
            g = []
            for j in i[5]:
                sql = '''
                    select gid from {gcp}
                    where st_intersects(st_point({x},{y},32643),st_buffer(geom,0.5));
                '''.format(
                    gcp=self.schema_name + "." + self.gcp,
                    x=j[0],
                    y=j[1]
                )
                with self.psql_conn.connection().cursor() as curr:
                    curr.execute(sql)
                    gcp_gid = curr.fetchall()
                    g.append(gcp_gid[0][0])
            print("Index :-", i[0], "GCPs :-", g,
                    "Excess_Area :-", i[2], "Distortion :-", i[3])

        results = list(filter(lambda x: x[3] < 100, results))
        results.sort(key=lambda x: x[2])

        if len(results) == 0:
            return math.inf

        i = results[0]

        recreate_table(self.psql_conn, self.schema_name, input, self.schema_name, output, i[1] )

        g = []
        for j in i[5]:

            sql = '''
                select gid from {gcp}
                where st_intersects(st_point({x},{y},32643),st_buffer(geom,0.5));
            '''.format(
                gcp=self.schema_name +"." + self.gcp,
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
    
    def fitting_source_target_points(self, degree, source_points, target_points, input, output):
        input_table = self.schema_name + "." + input

        if len(source_points) == 0:
            print("Error: No GCP match found")
            exit

        poly_X = PolynomialFeatures(degree=degree)
        in_features_X = poly_X.fit_transform(source_points)
        model_X = LinearRegression()
        model_X.fit(in_features_X, list(zip(*target_points))[0])

        poly_Y = PolynomialFeatures(degree=degree)
        in_features_Y = poly_Y.fit_transform(source_points)
        model_Y = LinearRegression()
        model_Y.fit(in_features_Y, list(zip(*target_points))[1])

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
        transformed_X = model_X.predict(poly_X.transform(points))
        transformed_Y = model_Y.predict(poly_Y.transform(points))
        transformed_points = [(i[0], (i[1][0], (j, k))) for i, j, k in zip(
            points_with_path, transformed_X, transformed_Y)]
        df = pd.DataFrame(transformed_points, columns=["gid", "path_point"])
        recreate_table(self.psql_conn, self.schema_name, input, self.temp_georeferencing_schema, output, df)
        excess_area = excess_area_without_parameters( self.psql_conn, self.temp_georeferencing_schema, output, self.farmplots, self.schema_name)
        distortion = get_distortion(self.psql_conn, self.temp_georeferencing_schema, output,  self.survey_processed, self.schema_name)

        return [df, excess_area, distortion]
    
    def run(self, degree):
        if self.gcp_label_toggle == "True":
            used_map = self.survey_shifted
        elif self.gcp_label_toggle == "False":
            used_map = self.survey_jitter
        self.output = self.survey_jitter_degree + str(degree)
        return self.survey_jitter_higher_order(used_map, self.gcp_map, self.output, degree)
        
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")
    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    parser.add_argument("-d", "--degree", help="Degree of polynomial function",
                        required=False, default="1")
    parser.add_argument("-v", "--village", help="Village",
                        required=False, default="")
    argument = parser.parse_args()
    gcp_label_toggle = argument.gcp_label_toggle
    village = argument.village
    poly = polynomial(village, gcp_label_toggle)
    degree = int(argument.degree)
    excess_area, gcps_used = poly.run(degree)
