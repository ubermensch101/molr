from config import *
from utils import *
import argparse

def validate_farmplots(config,psql_conn):
    village = config.setup_details['setup']['village']
    farmplots = village + '.' + config.setup_details['data']['farmplots_table']
    gcps = village + '.' + config.setup_details['data']['gcp_table']
    farm_topo = village+config.setup_details['fp']['farm_topo_suffix']
    
    avg_varp = add_varp(psql_conn, village, config.setup_details['data']['farmplots_table'], 'varp')
    num_overlap = num_of_overlapping_farmplots(psql_conn, farmplots)
    rms_error = check_rms_error(psql_conn, farm_topo, gcps)
    
    print("Average Varp :-", avg_varp)
    print("Average GCP Error :-", rms_error)
    print("Number of intersecting farmplots :-",num_overlap)
    
def check_rms_error(psql_conn, input_topo, gcps):
    sql = f'''
        with nodes as 
        (
            select st_collect(geom) as geom from {input_topo}.node
        )
        
        select 
            avg(st_distance(g.geom,n.geom))
        from 
            nodes as n,
            {gcps} as g
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        rating = curr.fetchall()
        if rating is None:
            return -1
        else:
            return rating[0][0]
    
def num_of_overlapping_farmplots(psql_conn, table):
    sql = f'''
        select 
            count(*)
        from 
            {table} as p 
        inner join
            {table} as q
        on p.gid != q.gid
        where
            st_intersects(p.geom,q.geom)
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        num_overlaps = curr.fetchall()
    return int(int(num_overlaps[0][0])/2)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    validate_farmplots(config, pgconn)
