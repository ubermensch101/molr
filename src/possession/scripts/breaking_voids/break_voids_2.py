from config import *
from utils import *
from scripts import *
import argparse

def break_voids_2(config, psql_conn, input_onwership_polygons, output_ownership_polygons):
    """ Function to break unasigned farmplots and voids
    """
    village = config.setup_details['setup']['village']
    shifted_topo = config.setup_details['pos']['topo']
    edge = config.setup_details['pos']['edge']
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
            survey_gid is NULL;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)
        res = curr.fetchall()
        
    for (gid,) in res:
        print(f"Processing {input_onwership_polygons} gid {gid}")
        sql_query = f'''
            drop table if exists {village}.temp;
            create table {village}.temp as
            select 
                (st_dump(
                    st_split(
                        (select geom from {input_onwership_polygons} where gid = {gid}),
                        (select st_union(geom) from {transformed_edges})
                    )
                )).geom as geom
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
                
        sql_query = f'''
            insert into {output_ownership_polygons} (geom)
                select geom from {village}.temp;
            update {output_ownership_polygons}
            set type = 'void'
            where type is NULL;

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
    
    break_voids_2(config, pgconn, input_table, output_table)