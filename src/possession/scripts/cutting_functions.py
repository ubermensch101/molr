from config import *
from utils import *
from scripts import *

def find_splitting_edge(psql_conn, gid, input_onwership_polygons, transformed_edges):
    # Find the shifted face edge by which the farmplot need to be cut
    sql_query = f'''
        with farmplots as (
            select
                geom
            from
                {input_onwership_polygons}
            where
                gid = {gid}
        )

        select
            edge_id
        from
            {transformed_edges} edges,
            farmplots
        where
            st_length(
                st_intersection(edges.geom, farmplots.geom)
            ) > sqrt(st_area(farmplots.geom)) / 3
            or st_length(st_intersection(edges.geom, farmplots.geom)) > 200
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)
        survey_edge_ids = curr.fetchall()
            
    area_cut_list = []
    for (edge_id,) in survey_edge_ids:
        ratio = area_cut_ratio(psql_conn, edge_id, gid, input_onwership_polygons, transformed_edges)
        if ratio == 0:
            continue
        area_cut_list.append([edge_id,ratio])

    area_cut_list.sort(key=lambda x:x[1])
    print(area_cut_list)
    if len(area_cut_list) == 0:
        return -1
    return area_cut_list[0][0]

def area_cut_ratio(psql_conn, edge_id, gid, input_onwership_polygons, transformed_edges):
    sql_query = f'''
        select 
            st_area(
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
                                    ST_Azimuth(a, b) AS az1, 
                                    ST_Azimuth(b, a) AS az2, 
                                    ST_Distance(a, b) + 200 AS len
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
                )).geom
            );
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)
        areas = curr.fetchall()
    print("areas",areas)
    if len(areas) >=2:
        area_list = [area for (area,) in areas]
        area_list.sort()
        area_ratio =  area_list[-1]/area_list[-2]
        print("edge_id",edge_id, area_ratio)
        return area_ratio
    return 0

def polygonize(psql_conn, input_table, output_table):
    sql_query = f'''
        drop table if exists {output_table};
        create table {output_table} as
        select
            (st_dump(
                (
                    st_polygonize(geom)
                )
            )).geom
        from {input_table}
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)

def narrow_face_creator(config, psql_conn):
    village = config.setup_details['setup']['village']
    shifted_faces = config.setup_details['pos']['shifted_faces']
    narrow_faces = config.setup_details['pos']['narrow_faces']
    sql_query=f"""
        drop table if exists {village}.{narrow_faces};
        create table {village}.{narrow_faces} as
        
        select geom                
        from
            {village}.{shifted_faces}
        where
            st_area(geom)>1
            and
            st_perimeter(geom) * 
                st_perimeter(geom) /
                st_area(geom) > 55
        ;
    """
    with psql_conn.connection().cursor() as curs:
        curs.execute(sql_query)
    psql_conn.connection().commit()