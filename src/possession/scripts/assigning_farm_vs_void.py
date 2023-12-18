from config import *
from utils import *

def assign_type_farm_vs_void(config, psql_conn):
    """Assgins type as farm or void and calls the create super_poly function that assigns ownership 
    and takes union of farm faces"""
    
    village = config.setup_details['setup']['village']
    ownership_polygons = config.setup_details['pos']['ownership']
    shifted_faces = config.setup_details['pos']['shifted_faces']
    farm_faces = config.setup_details['pos']['farm_faces']
    farmplots = config.setup_details['pos']['farmplots']
    
    sql_query = f'''
    
        drop table if exists {village}.{ownership_polygons};
        create table {village}.{ownership_polygons} as
        with filter as 
        (
            select st_union(geom) as geom from {village}.{shifted_faces}
        )
        select * from {village}.{farm_faces}
        where 
            st_area(geom)>1
            and
            st_intersects(
                geom,
                (select f.geom from filter as f)
            )
        ;
        
        delete from {village}.{ownership_polygons}
        where st_area(geom)<1;
        -- type can be void, farm
        alter table {village}.{ownership_polygons}
        add column type varchar;
        update {village}.{ownership_polygons} as p
        set type = 
        CASE
            WHEN 
                st_area(
                    st_intersection(
                        p.geom,(
                            select 
                            st_union(geom) 
                            from {village}.{farmplots}
                        )
                    )
                ) < 0.1*st_area(geom)
            THEN 'void'
            ELSE 'farm'
        END;    
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)