def find_tri_junctions(psql_conn, schema , topo_name, output):
    input_topo_schema = schema + "." + topo_name
    output_table = schema + "." + output
    sql = f'''
        drop table if exists {output_table};
        create table {output_table} as

        with neigh as (
            select
                count(edge_id),
                node_id
            from
                {input_topo_schema}.edge as p,
                {input_topo_schema}.node
            where
                start_node = node_id
                or end_node = node_id
            group by
                node_id
        )

        select
            r.node_id as node_id,
            r.geom as geom
        from
            {input_topo_schema}.node as r,
            neigh
        where
            r.node_id = neigh.node_id
            and
            neigh.count > 2
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)

        