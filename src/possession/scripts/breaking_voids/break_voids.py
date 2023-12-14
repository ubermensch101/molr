from config import *
from src.possession.scripts.cutting_functions import find_splitting_edge
from utils import *
from scripts import *
import argparse

def break_voids(config, psql_conn, input_onwership_polygons, output_ownership_polygons):
    """ Function to break unassigned farmplots and voids
    """
    # transformed_edges = self.topo_name + ".edge"
    # self.topo_name = input_topo
    # input_topo = topo = f'{village}_shifted_topo'
    # transformed_edges = {village}_shifted_topo.edge
    shifted_topo = config.setup_details['pos']['topo']
    edge = config.setup_details['pos']['edge']
    village = config.setup_details['setup']['village']
    transformed_edges = f'{village}_{shifted_topo}.{edge}'

    sql_query = f'''
        drop table if exists {output_ownership_polygons};
        create table {output_ownership_polygons}
        as select * from {input_onwership_polygons};
        
        select 
            gid 
        from 
            {input_onwership_polygons}
        where
            survey_gid is NULL
            and 
            st_area(geom) > 10000    
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)
        res = curr.fetchall()
    
    splitting_at_edges(config, psql_conn, res, input_onwership_polygons, transformed_edges, output_ownership_polygons)
        
def splitting_at_edges(config, psql_conn, res, input_onwership_polygons, transformed_edges, output_ownership_polygons):
    village = config.setup_details['setup']['village']
    for (gid,) in res:
        print(f"Processing {input_onwership_polygons} gid {gid}")
        edge_id = find_splitting_edge(psql_conn, gid, input_onwership_polygons, transformed_edges)
        if edge_id == -1:
            continue
        print("chosen edge id",edge_id)
        
        sql_query = f'''
            drop table if exists {village}.temp;
            create table {village}.temp as
            select 
                (st_dump(
                    st_split(
                        (select geom from {input_onwership_polygons} where gid = {gid}),
                        (
                            SELECT 
                                ST_MakeLine(
                                    ST_TRANSLATE(a, sin(az1) * len, cos(az1) * len),
                                    ST_TRANSLATE(b, sin(az2) * len, cos(az2) * len)
                                )
                            FROM 
                            (
                                SELECT 
                                    a, 
                                    b, 
                                    ST_Azimuth(a,b) AS az1, 
                                    ST_Azimuth(b, a) AS az2, 
                                    ST_Distance(a,b) + 200 AS len
                                FROM 
                                (
                                    SELECT 
                                        ST_StartPoint(geom) AS a, 
                                        ST_EndPoint(geom) AS b
                                    from 
                                        {transformed_edges} 
                                    where 
                                        edge_id = {edge_id}
                                ) AS sub
                            ) AS sub2
                        )
                    )
                )).geom as geom,
                (select type from {input_onwership_polygons} where gid = {gid}) as type
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
                
        sql_query = f'''
            insert into {output_ownership_polygons} (geom,type)
                select geom,type from {village}.temp;

            delete from {output_ownership_polygons}
            where gid = {gid};
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    sql_query = f'''
        alter table {output_ownership_polygons}
        drop column gid;

        alter table {output_ownership_polygons}
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
    shifted_faces = f'{village}.shifted_faces'
        
    config = Config()
    pgconn = PGConn(config)

    input_table = config.setup_details['pos']['input_table']
    output_table = config.setup_details['pos']['output_table']
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    break_voids(config, pgconn, input_table, output_table)