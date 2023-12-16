import pandas as pd
import numpy as np
import re
import math

def recreate_table(psql_conn, inp_schema , input , ou_schema, output, input_df):
    input_table = inp_schema + "." + input
    output_table = ou_schema + "." + output
    df_new = input_df.groupby("gid", as_index=False).agg(list)
    strings = []
    for index, i in df_new.iterrows():
        dat = i["path_point"]
        
        max_size = [max([k[0] for k in dat], key=lambda x: x[j])[j]
                    for j in range(3)]
        lst = np.full(tuple(max_size), None).tolist()
        for k in dat:
            point_x = float(format(k[1][0],".6f"))
            point_y = float(format(k[1][1],".6f"))

            lst[k[0][0]-1][k[0][1]-1][k[0][2]-1] = f"{point_x} {point_y}"
        for x in lst:
            for y in x:
                while None in y:
                    y.remove(None)
                if len(y) == 0:
                    x.remove(y)
            if len(x) == 0:
                lst.remove(x)
        lst_tuple = tuple(tuple(tuple(w) for w in u) for u in lst)
        query = "MULTIPOLYGON " + \
            re.sub(',\)', ')', re.sub("[\'\"]", "", str(lst_tuple)))

        strings.append((i["gid"], query))

    sql = f'''
        SELECT postgis_typmod_type(atttypmod)
        FROM pg_attribute
        WHERE attrelid = '{input_table}'::regclass
        AND attname = 'geom';
    '''

    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        x = curr.fetchall()

    dim_force = "st_force4d" if x[0][0] == "MultiPolygonZM" else (
        "st_force3d" if x[0][0] == "MultiPolygonZ" else "st_force2d")

    sql = f'''
        DROP TABLE IF EXISTS {output_table};
        CREATE TABLE {output_table} as TABLE {input_table};
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)

    for i in strings:
        sql = f'''
            UPDATE {output_table}
            SET geom=({dim_force}(st_geomfromtext('{i[1]}',32643)))
            WHERE gid={i[0]}
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)


def get_farmplot_areas_at_boundary(psql_conn, schema, input, farmplots, farm_schema= None):
    if farm_schema is None:
        farm_schema = schema
    input_table = schema + "." + input
    farmplots = farm_schema + "." + farmplots
    sql = f'''
        select
            st_area(st_union(f.geom))
        from (select st_transform(geom,32643) as geom from {farmplots}) as f,
        (select st_boundary(st_union(geom)) as geom from {input_table}) as input
        where st_intersects(input.geom, f.geom);
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        area = curr.fetchone()
        if area is None:
            return math.inf
        else:
            return float(area[0])
        



def validate_geom(psql_conn, schema , input):
    input_table = schema + "." + input
    sql_query = """
            update {input_table}
            set geom = st_makevalid(geom, 'method=structure')
            where not st_isvalid(geom)
            ;
        """.format(input_table=input_table)

    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)
        

def fix_gaps(psql_conn, schema , input):
        input_table = schema + "." + input
        sql = '''
        drop table if exists {schema_name}.filling_narrow_sections;
        create table {schema_name}.filling_narrow_sections
        as
        select 
            st_difference(
                st_makepolygon(
                    st_exteriorring(
                        st_buffer(
                            st_buffer(
                                st_union(geom),
                                10
                            ),
                            -10
                        )
                    )
                ),
                st_union(geom)
            ) as geom 
        from {input_table};
            
        drop table if exists {schema_name}.new_narrow_sections;
        create table {schema_name}.new_narrow_sections 
        as 
        select 
            st_buffer(
                st_buffer(
                    (st_dump(st_polygonize(geom))).geom,
                    -0.2),
                    0.2
                ) 
        as geom from {schema_name}.filling_narrow_sections;
            
        delete from {schema_name}.new_narrow_sections 
        where st_area(geom)<100; 
            
        alter table {schema_name}.new_narrow_sections 
        add column if not exists id serial;
            
        insert into {input_table} (survey_no,geom)
        select ('NN' || id ),st_multi(geom) from {schema_name}.new_narrow_sections;

        alter table {input_table}
        drop column if exists gid;
            
        alter table {input_table}
        add column gid serial;
            
        
        '''.format(input_table= input_table ,
                   schema_name=schema)
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)