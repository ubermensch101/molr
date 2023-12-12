import numpy as np
from .postgres_utils import *

def add_akarbandh(psql_conn, input_table, akarbandh_table, akarbandh_col):        
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
        set {akarbandh_col} = (select area from {akarbandh_table} where survey_no = p.survey_no)
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def calculate_varp_of_individual( points_list):
    points = points_list
    points.append(points_list[1])
    sum = 0
    for i in range(len(points)-2):
        a = np.array([float(points[i][0]), float(points[i][1])])
        b = np.array([float(points[i+1][0]), float(points[i+1][1])])
        c = np.array([float(points[i+2][0]), float(points[i+2][1])])

        ba = b - a
        bc = c - b

        cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
        angle = np.arccos(cosine_angle)
        sum += abs(angle)

    return sum/(2*np.pi)

def get_all_gids(psql_conn, input_table):
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
        
def add_varp(psql_conn, input_table):
    farm_gids = get_all_gids(psql_conn, input_table)
    
    add_column(psql_conn, input_table, 'varp', 'float')
    
    varp_sum = 0
    
    for farm_gid in farm_gids:
        sql = f'''
                select st_x(
                    (st_dumpPoints(geom)).geom),
                    st_y(
                        (st_dumpPoints(geom)).geom) 
                from 
                    {input_table} 
                where
                    gid = {farm_gid};
            '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            res = curr.fetchall()
        varp = calculate_varp_of_individual(res)
        varp_sum += varp
        sql = f'''
            update {input_table}
            set varp = {varp}
            where gid = {farm_gid};
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    return varp_sum/len(farm_gids)
    