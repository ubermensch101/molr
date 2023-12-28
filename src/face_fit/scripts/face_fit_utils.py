from utils import *

def add_to_shifted_nodes(psql_conn, input_table, shifted_nodes_table, input_table_id_column='node_id',
                         input_table_geom_column='geom', srid=32643):
    sql = f"""
        create table if not exists {input_table} (node_id integer, geom geometry(Point,{srid}));
        
        insert into {shifted_nodes_table}
        select {input_table_id_column}, {input_table_geom_column} from {input_table};
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def create_nodes_table(psql_conn, table, srid = 32643):
    sql = f"""
        drop table if exists {table};
        create table {table} (
            node_id integer,
            geom geometry(Point, {srid})
        );
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)