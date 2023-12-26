from .postgres_utils import *
      
def get_corner_nodes(psql_conn, input_topo_schema, output_schema, output_table, angle_thresh=45, only_trijunctions=False):
    angle_thresh = int(angle_thresh)
    sql = f'''
        drop table if exists {output_schema}.{output_table};
        create table {output_schema}.{output_table} as

        with neigh as (
            select
                count(p.edge_id) as count,
                n.node_id as node_id,
                n.geom as geom
            from
                {input_topo_schema}.edge as p,
                {input_topo_schema}.node as n
            where
                p.start_node = n.node_id
                or 
                p.end_node = n.node_id
            group by
                n.node_id
        )

        select
            node_id,
            geom
        from
            neigh
        where
            count > 2
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
    if not only_trijunctions:
        bounds = [angle_thresh, 180-angle_thresh, 180+angle_thresh, 360-angle_thresh]
        sql_query = f"""
            insert into {output_schema}.{output_table}

            with neigh as (
                select
                    count(p.edge_id) as count,
                    n.node_id as node_id,
                    n.geom as geom
                from
                    {input_topo_schema}.edge as p,
                    {input_topo_schema}.node as n
                where
                    p.start_node = n.node_id
                    or 
                    p.end_node = n.node_id
                group by
                    n.node_id
            )
            
            select 
                n.node_id,
                n.geom
            from 
                {input_topo_schema}.edge_data as p
            join 
                {input_topo_schema}.edge_data as q 
                on 
                    p.start_node = q.end_node
            join 
                neigh as n 
                on 
                    p.start_node = n.node_id    
            where
                n.count = 2
                and
                (
                    (
                        degrees(st_angle(p.geom,q.geom)) > {bounds[0]}
                        and 
                        degrees(st_angle(p.geom,q.geom)) < {bounds[1]}
                    )
                    or
                    (
                        degrees(st_angle(p.geom,q.geom)) > {bounds[2]}
                        and 
                        degrees(st_angle(p.geom,q.geom)) < {bounds[3]}
                    )
                )
            ;
        """
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            
def process_labels(inp_list, replacement_dictionary):
    replaced = list(map(lambda x: replacement_dictionary.get(next((prefix for prefix in replacement_dictionary if x.startswith(prefix)), x), x), inp_list))
    return "-".join(sorted(replaced))
    
def create_node_labels(psql_conn, schema, input_table, nodes_table, 
                       reference_column='survey_no', nodes_label_column='label', 
                       buf_thresh='0.1', village_boundary_label=None, 
                       label_update_dictionary = {'G':'g', 'S':'rv', 'R':'rd'}):
    if not check_column_exists(psql_conn, schema, input_table, reference_column):
        print(f"Column {reference_column} does not exist")
        return
    add_column(psql_conn, schema+'.'+nodes_table, nodes_label_column, 'varchar(100)')
    
    if village_boundary_label != None:
        add_village_boundary(psql_conn, schema, input_table, reference_column, village_boundary_label)
    
    sql = f"""
        select 
            nodes.node_id,
            array_agg(inp.{reference_column})
        from 
            {schema}.{input_table} as inp,
            {schema}.{nodes_table} as nodes
        where
            st_dwithin(nodes.geom, inp.geom, {buf_thresh})
        group by
            nodes.node_id;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        label_dump = curr.fetchall()
        
    labels = dict(map(lambda x: (x[0], process_labels(x[1], label_update_dictionary)), label_dump))
    
    for key, val in labels.items():
        if val==None:
            continue
        sql = f"""
            update {schema}.{nodes_table}
            set {nodes_label_column} = '{val}'
            where node_id = {key};
        """
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
    
    if village_boundary_label != None:
        remove_village_boundary(psql_conn, schema, input_table, reference_column, village_boundary_label)

def create_gcp_map(psql_conn, schema, nodes_table, gcp_table, output_map_table, nodes_label_column='label', gcp_label_column='asno', use_labels=True, delimiter_regex='-|,'):
    if use_labels:
        if not check_column_exists(psql_conn, schema, gcp_table, gcp_label_column):
            print(f"Column {gcp_label_column} does not exist")
            return
        if not check_column_exists(psql_conn, schema, nodes_table, nodes_label_column):
            print(f"Column {nodes_label_column} does not exist")
            return
        
        sql = f"""
            create or replace function 
                sort_and_format_label
                (
                    input_label varchar, 
                    delimiter_regex varchar
                )
            returns varchar as
            $$
            begin
                return array_to_string(
                    array(
                        select elem
                        from unnest(regexp_split_to_array(input_label, delimiter_regex)) as elem
                        order by elem
                    ),
                    '-'
                );
            end;
            $$ language plpgsql;
        """
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        
        sql = f"""
            drop table if exists {schema}.{output_map_table};
            create table {schema}.{output_map_table} as 
            
            select
                g.gid as gid,
                g.geom as gcp_geom,
                n.node_id as node_id,
                n.geom as node_geom
            from 
                {schema}.{gcp_table} as g,
                {schema}.{nodes_table} as n
            where
                sort_and_format_label(g.{gcp_label_column},'{delimiter_regex}') 
                = 
                sort_and_format_label(n.{nodes_label_column},'{delimiter_regex}');
        """
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
    
    else:
        create_distance_gcp_map()
        
def create_distance_gcp_map(psql_conn,  schema, nodes_table, gcp_table, output_map_table, distance_thresh=30):
    
    sql = f'''
        drop table if exists {schema}.{output_map_table};
        create table {schema}.{output_map_table} as
        
        with gcp_collect as (
            select
                st_collect(geom) as geom
            from
                {schema}.{gcp_table}
        )

        select
            g.gid as gid,
            g.geom as gcp_geom,
            n.node_id as node_id,
            n.geom as node_geom
        from
            {schema}.{nodes_table} as n,
            {schema}.{gcp_table} as g,
            gcp_collect as c
        where
            st_closestpoint(c.geom, n.geom) = g.geom
            and
            st_distance(gcp.geom,point.geom) < {distance_thresh};
            
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def add_gcp_label(psql_conn, schema, nodes_table, gcp_table, output_map_table, 
                  nodes_label_column='survey_no', gcp_label_column='asno', overwrite=False):
    
    if not overwrite and check_column_exists(psql_conn, schema, gcp_table, gcp_label_column):
        print(f"Column {gcp_label_column} already exists and can't overwrite, skipping")
        
    add_column(psql_conn, schema+'.'+gcp_table, gcp_label_column, 'varchar(100)')
    
    if not check_column_exists(psql_conn, schema, nodes_table, nodes_label_column):
        print(f"Column {nodes_label_column} does not exist")
        return

    sql = f"""
        update {schema}.{gcp_table} as g
        set {gcp_label_column} = (
            select 
                {nodes_label_column}
            from 
                {schema}.{output_map_table} as o,
            join
                {schema}.{nodes_table} as n
                on
                    n.node_id = o.node_id
            where
                o.gid = g.gid
        );

    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
    

def add_village_boundary(psql_conn, schema, input_table, reference_column, label='vb', vb_buf='10'):
    table = schema + '.' + input_table
    
    sql = f"""
        select 
            exists(
                select 
                    1 
                from 
                    {table} 
                where 
                    {reference_column} = '{label}'
            )
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        label_exists = curr.fetchone()[0]

    if label_exists:
        print(f"Label '{label}' already exists in the table. Choose a different label.")
        return
    
    sql = f'''
        with combined as (
            select 
                st_union(geom) as geom
            from 
                {table}
        ),
        bounding_box as (
            select  
                st_expand(
                    geom,
                    {vb_buf}
                ) as geom 
            from 
                combined
        ),
        outer_polygon as (
            select
                st_multi(
                    st_difference(
                        b.geom,
                        st_makepolygon(st_exteriorring(u.geom))  
                    )
                ) as geom
            from
                bounding_box as b,
                combined as u
        ) 
        insert into {table} ({reference_column},geom)
        select 
            '{label}', 
            geom 
        from 
            outer_polygon;
    '''
    with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
def remove_village_boundary(psql_conn, schema, input_table, reference_column, label='vb'):
    table = schema+"."+input_table
    
    sql = f'''
        delete from {table}
        where
        {reference_column} = '{label}';
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)