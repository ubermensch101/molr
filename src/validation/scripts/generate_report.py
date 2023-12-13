from config import *
from utils import *
import pandas as pd
import argparse

def add_report(config, psql_conn):

    village = config.setup_details['setup']['village']
    report = config.setup_details['val']['report_table']
    survey_georef = config.setup_details['data']['survey_georeferenced_table']
    shifted = config.setup_details['data']['shifted_faces_table']
    possession = config.setup_details['data']['possession_table']
    survey_no = config.setup_details['val']['survey_no_label']
    akarbandh_col = config.setup_details['data']['survey_map_akarbandh_col']
    farm_intersection_thresh = config.setup_details['val']['farm_intersection_thresh']
    akarbandh_validity_thresh = config.setup_details['val']['akarbandh_validity_thresh']
    
    sql = f"""
        drop table if exists {village}.{report};
        create table {village}.{report} (
            {survey_no} varchar(100),
            valid bool,
            akarbandh_validity bool,
            akarbandh_area float,
            georeferenced_area float,
            refined_area float,
            possession_area float,
            geo_with_akarbandh float,
            ref_with_akarbandh float,
            pos_with_akarbandh float,
            ref_with_geo_area float,
            ref_with_geo_perimeter float,
            ref_with_geo_deviation float,
            pos_with_geo_area float,
            pos_with_geo_perimeter float,
            pos_with_geo_deviation float,
            pos_with_ref_area float,
            pos_with_ref_perimeter float,
            pos_with_ref_deviation float,
            geo_farm_rating float,
            ref_farm_rating float,
            pos_farm_rating float,
            geo_varp float,
            ref_varp float,
            pos_varp float,
            ag bool
        );
        
        insert into {village}.{report} 
            ({survey_no}, valid, {akarbandh_col},
            georeferenced_area, geo_with_akarbandh,
            akarbandh_validity, geo_farm_rating,
            geo_varp, ag) 
        select 
            {survey_no},
            valid,
            {akarbandh_col},
            st_area(geom)/10000 as georeferenced_area,
            akarbandh_area_diff*100 as geo_with_akarbandh,
            CASE
            WHEN abs(akarbandh_area_diff*100) > {akarbandh_validity_thresh} THEN false
            ELSE true
            END as akarbandh_validity,
            farm_rating*100 as geo_farm_rating,
            varp as geo_varp,
            CASE
            WHEN abs(farm_intersection*100) > {farm_intersection_thresh} THEN true
            ELSE false
            END as ag
        from
            {village}.{survey_georef}
        ;
        
        update {village}.{report} r
        set 
            refined_area = st_area(s.geom)/10000,
            ref_with_akarbandh = s.akarbandh_area_diff*100,
            ref_with_geo_area = s.area_diff*100,
            ref_with_geo_perimeter = s.perimeter_diff*100,
            ref_with_geo_deviation = s.deviation*100,
            ref_farm_rating = s.farm_rating*100,
            ref_varp = s.varp
        from 
            {village}.{shifted} as s
        where
            r.valid = true
            and
            r.{survey_no} = s.{survey_no};

        update {village}.{report} r
        set 
            possession_area = st_area(s.geom)/10000,
            pos_with_akarbandh = s.akarbandh_area_diff*100,
            pos_with_geo_area = s.area_diff*100,
            pos_with_geo_perimeter = s.perimeter_diff*100,
            pos_with_geo_deviation = s.deviation*100,
            pos_farm_rating = s.farm_rating*100,
            pos_varp = s.varp
        from 
            {village}.{possession} as s
        where
            r.valid = true
            and
            r.{survey_no} = s.{survey_no};
            
        update {village}.{report} r
        set 
            pos_with_ref_area = ((st_area(s.geom) - st_area(sh.geom))/st_area(sh.geom))*100,
            pos_with_ref_perimeter = ((st_perimeter(s.geom) - st_perimeter(sh.geom))/st_perimeter(sh.geom))*100,
            pos_with_ref_deviation = ((st_area(st_difference(s.geom, sh.geom)) + st_area(st_difference(sh.geom, s.geom)))/(2*st_area(sh.geom))) *100
        from 
            {village}.{possession} as s, {village}.{shifted} as sh
        where
            r.valid = true
            and
            r.{survey_no} = s.{survey_no}
            and 
            r.{survey_no} = sh.{survey_no};
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)
    
    config.setup_details['setup']['village'] = village
    
    add_report(config, pgconn)