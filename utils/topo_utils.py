from .postgres_utils import *

def get_geom_type(psql_conn, table):
    sql = f"""
        select geometrytype(geom) as geometry_type
        from {table}
        limit 1;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        type = curr.fetchone()
        if type is None:
            print("ERROR")
            exit()
        
    return type[0]
    

def create_topo(psql_conn, schema, topo_schema, input_table, tol=0):
    
    type = get_geom_type(psql_conn, schema+'.'+input_table)
    
    comment = "" if check_schema_exists(psql_conn, topo_schema) else "--"
    
    sql=f"""
        {comment}select DropTopology('{topo_schema}');
        select CreateTopology('{topo_schema}', 32643, {tol});
        
        drop table if exists {schema}.{input_table}_t;
        create table {schema}.{input_table}_t as table {schema}.{input_table};
        
        select AddTopoGeometryColumn('{topo_schema}', '{schema}', '{input_table}_t','topo', '{type}');
        
        update {schema}.{input_table}_t
        set topo = totopogeom(geom,'{topo_schema}',layer_id(findlayer('{schema}','{input_table}_t','topo')));

        with points as (
            select
                (st_dumppoints(geom)).geom as geom
            from 
                {topo_schema}.edge_data
        ) 
        select TopoGeo_AddPoint('{topo_schema}',geom, {tol}) from points;
    """
    
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)

def polygonize_topo(psql_conn, schema, topo_name, output):
    sql_query=f"""
        drop table if exists {schema}.{output};
        create table {schema}.{output} as 
        with edges as 
            (
                select 
                    st_collect(geom) as geom
                from
                    {topo_name}.edge_data
            )
        select 
            st_multi((st_dump(st_polygonize(geom))).geom) as geom
        from 
            edges;
            
        alter table {schema}.{output}
        add column gid serial;
            
    """

    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)