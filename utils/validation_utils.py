import numpy as np
from .postgres_utils import *

def add_akarbandh(psql_conn, input_table, akarbandh_table, akarbandh_col, common_col):        
    sql = f'''
        alter table {input_table}
        drop column if exists {akarbandh_col};

        alter table {input_table}
        add column {akarbandh_col} float;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
    sql = f'''
        update {input_table} as p
        set {akarbandh_col} = (select area from {akarbandh_table} where {common_col} = p.{common_col})
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def add_farm_rating(psql_conn, schema, input_table, farmplots, column_name):
    pass
        
def calculate_varp_of_individual(points_list):
    points = points_list
    points.append(points_list[1])
    sum = 0
    for i in range(len(points)-2):
        a = np.array([float(points[i][0]), float(points[i][1])])
        b = np.array([float(points[i+1][0]), float(points[i+1][1])])
        c = np.array([float(points[i+2][0]), float(points[i+2][1])])

        ba = b - a
        bc = c - b
        if (np.linalg.norm(ba) * np.linalg.norm(bc)) == 0 or np.dot(ba, bc)==0:
            continue
        cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
        angle = np.arccos(min(cosine_angle,1))
        sum += abs(angle)

    return sum/(2*np.pi)

def get_all_gids(psql_conn, input_table):
    schema = input_table.split('.')[0]
    table = input_table.split('.')[1]
    
    if not check_column_exists(psql_conn, schema, table, 'gid'):
        print('GID does not exist')
        exit()
        
    sql = f'''
        select gid from {input_table};
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        a = curr.fetchall()

    farm_gids = []

    for res in a:
        farm_gids.append(int(res[0]))
    
    return farm_gids
        
def add_varp(psql_conn, schema, input_table, column_name):
    farm_gids = get_all_gids(psql_conn, schema+'.'+input_table)
    
    add_column(psql_conn, schema+'.'+input_table, f'{column_name}', 'float')
    
    varp_sum = 0
    
    for farm_gid in farm_gids:
        sql = f'''
                select st_x(
                    (st_dumpPoints(geom)).geom),
                    st_y(
                        (st_dumpPoints(geom)).geom) 
                from 
                    {schema}.{input_table} 
                where
                    gid = {farm_gid};
            '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            res = curr.fetchall()
        varp = calculate_varp_of_individual(res)
        if np.isnan(varp):
            varp = 1
            varp_sum += varp
            continue
        varp_sum += varp
        sql = f'''
            update {schema}.{input_table}
            set {column_name} = {varp}
            where gid = {farm_gid};
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    return varp_sum/len(farm_gids)
    

def list_overlaps(psql_conn, schema, table, column):
    add_gist_index(psql_conn, schema, table, 'geom')
    if not check_column_exists(psql_conn,schema, table, column):
        print('GID does not exist')
        exit()
    sql = f"""
        select      
            a.{column} AS polygon1_id,     
            b.{column} AS polygon2_id, 	
            st_area(st_intersection(a.geom, b.geom)) as area 
        from
            {schema}.{table} a,
            {schema}.{table} b 
        where      
            a.{column} < b.{column}      
            and 
            a.geom && b.geom
            and
            st_area(st_intersection(a.geom, b.geom)) != 0
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        intersections = curr.fetchall()
    
    return intersections


def add_survey_no(psql_conn, schema, input_table, ref_table, column_name, intersection_thresh):

        sql_query = f"""
            alter table {schema}.{input_table}
            add column if not exists {column_name} varchar(100) default '';

            update {schema}.{input_table} partition
            set {column_name} = original.{column_name}
            from
                {schema}.{ref_table} original
            where
                st_intersects(partition.geom, original.geom)
                and
                st_area(st_intersection(partition.geom, original.geom))/
                    st_area(partition.geom) > {intersection_thresh}
            ;
            
            drop table if exists {schema}.temp;
            create table {schema}.temp as 
            select 
                {column_name} as {column_name},
                st_union(geom) as geom
            from 
                {schema}.{input_table} as p
            group by {column_name};
            
            drop table {schema}.{input_table};
            create table {schema}.{input_table} as table {schema}.temp;
            alter table {schema}.{input_table}
            add column if not exists gid serial;
            
        """

        with psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)

    