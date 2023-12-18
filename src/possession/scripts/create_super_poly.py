from config import *
from utils import *
from scripts import *
import argparse

def create_super_poly(config, psql_conn, input_table, output_table):
    """Input table -- farm graph polygons
    Output table --- super polygons table name(will be created in the function)"""

    # void_abs_threhold = 3000
    shifted_faces = config.setup_details['pos']['shifted_faces']
    void_percent_threshold = config.setup_details['pos']['void_percent_threshold']
    village = config.setup_details['setup']['village']
    ratio_inclusion_small = config.setup_details['pos']['ratio_inclusion_small']
    ratio_inclusion_large = config.setup_details['pos']['ratio_inclusion_large']

    sql_query = f'''
        alter table {input_table}
        add column if not exists survey_gid int;
        
        update {input_table} as p
        set survey_gid = 
        CASE
            WHEN type = 'farm'
            THEN
            (
                select gid from {village}.{shifted_faces} as q
                where 
                (st_area(p.geom) < 10000 
                and
                st_area(
                    st_intersection(
                        p.geom, q.geom
                    )
                ) > {ratio_inclusion_small}*st_area(p.geom))
                or  
                (st_area(
                    st_intersection(
                        p.geom, q.geom
                    )
                ) > {ratio_inclusion_large}*st_area(p.geom))
                order by st_area(st_intersection(p.geom, q.geom)) desc
                limit 1
            )
            ELSE
            (
                select gid from {village}.{shifted_faces} as q
                where 
                    st_area(st_intersection(p.geom, q.geom)) > {void_percent_threshold}*st_area(p.geom)
                order by st_area(st_intersection(p.geom, q.geom)) desc
                limit 1
            )
        END
        where survey_gid is NULL
        ;
        
        drop table if exists {output_table};
        create table {output_table} as 
        select 
            survey_gid,
            st_buffer(
                st_buffer(
                    st_union(geom), 
                    0.01
                ), 
                -0.01
            ) as geom 
            from {input_table} 
            where survey_gid is not null
            group by survey_gid;
        
        alter table {output_table}
        add column gid serial;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query) 

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)

    input_table = config.setup_details['pos']['input_table']
    output_table = config.setup_details['pos']['output_table']
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    create_super_poly(config, pgconn, input_table, output_table)