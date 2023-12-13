from config import *
from utils import *
import argparse

def setup_validate(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Setup_Validate(config,pgconn)

class Setup_Validate:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = config.setup_details['setup']['village']
        self.survey_no_label = config.setup_details['val']['survey_no_label']
        self.survey_georef = config.setup_details['data']['survey_georeferenced_table']
        self.shifted = config.setup_details['data']['shifted_faces_table']
        self.possession = config.setup_details['data']['possession_table']
        self.fp = config.setup_details['data']['farmplots_table']
        self.intersection_thresh = config.setup_details['val']['intersection_thresh']
        self.akarbandh_col = config.setup_details['data']['survey_map_akarbandh_col']
    
    def add_stats(self, schema, table, farmplots):
        add_varp(self.psql_conn, schema, table, 'varp')
        sql = f'''
            alter table {schema}.{table}
            add column if not exists shape_index float,
            add column if not exists farm_intersection float,
            add column if not exists farm_rating float;
            
            update {schema}.{table} a
            set farm_rating = (
                select 
                    avg(
                        greatest(
                            st_area(
                                st_intersection(
                                    a.geom,
                                    b.geom
                                )
                            )/st_area(b.geom),
                            st_area(
                                st_difference(
                                    b.geom,
                                    a.geom
                                )
                            )/st_area(b.geom)
                        )
                    )
                from
                    {schema}.{farmplots} as b
                where
                    st_intersects(st_buffer(st_boundary(a.geom),20), b.geom)
            ),
            farm_intersection = (
                select 
                    st_area(
                        st_intersection(
                            a.geom,
                            b.geom
                        )
                    )/st_area(a.geom)
                from 
                    (select st_collect(geom) as geom from {schema}.{farmplots}) as b
            ),
            shape_index = st_perimeter(geom)*st_perimeter(geom)/st_area(geom);
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    def validate_georeferenced(self, georef_table, akarbandh_area_col):
        sql = f"""
            alter table {georef_table}
            add column if not exists akarbandh_area_diff float;

            update {georef_table} a
            set akarbandh_area_diff = ((st_area(geom)/10000)-{akarbandh_area_col})/{akarbandh_area_col}
            where 
                {akarbandh_area_col} is not null
                and 
                {akarbandh_area_col} > 0;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        
    def validate_with_georeferenced(self, table, georef_table, common_col, akarbandh_area_col):
        sql = f"""
            alter table {table}
            add column if not exists akarbandh_area_diff float,
            add column if not exists area_diff float,
            add column if not exists perimeter_diff float,
            add column if not exists deviation float;
            
            UPDATE {table} AS t
            SET akarbandh_area_diff = ((st_area(t.geom)/10000) - subquery.{akarbandh_area_col})/subquery.{akarbandh_area_col},
                area_diff = (st_area(t.geom) - st_area(subquery.geom))/st_area(subquery.geom),
                perimeter_diff = (st_perimeter(t.geom) - st_perimeter(subquery.geom))/st_perimeter(subquery.geom),
                deviation = (st_area(st_difference(t.geom, subquery.geom)) + st_area(st_difference(subquery.geom, t.geom)))/(2*st_area(subquery.geom)) 
               
            FROM (
                SELECT {common_col},
                    {akarbandh_area_col},
                    geom
                FROM {georef_table}
                WHERE valid = true
            ) AS subquery
            WHERE t.{common_col} = subquery.{common_col};
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
              
    def run(self):
                
        self.add_stats(self.village, self.survey_georef, self.fp)
        add_survey_no(self.psql_conn, self.village, self.shifted, self.survey_georef, self.survey_no_label, self.intersection_thresh)
        add_survey_no(self.psql_conn, self.village, self.possession, self.survey_georef, self.survey_no_label, self.intersection_thresh)
        self.add_stats(self.village, self.shifted, self.fp)
        self.add_stats(self.village, self.possession, self.fp)
        self.validate_georeferenced(self.village+"."+self.survey_georef, self.akarbandh_col)
        self.validate_with_georeferenced(self.village+"."+self.shifted, self.village+"."+self.survey_georef, self.survey_no_label, self.akarbandh_col)
        self.validate_with_georeferenced(self.village+"."+self.possession, self.village+"."+self.survey_georef, self.survey_no_label, self.akarbandh_col)
            
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    
    datacorrecter = setup_validate()
    datacorrecter.run()