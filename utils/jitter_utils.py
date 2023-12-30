from scipy.optimize import minimize

def jitter_fit(psql_conn, schema, input, output, reference, option, bounds):
    #option - flag variable for deciding the type of jitter
    temp = "temp"
    print("CREATING SURVEY_JITTER")
    if option == 0:
        fit_with_excess_area(psql_conn, schema, input, output, reference, temp, bounds)
    elif option == 1:
        fit_with_excess_area_at_boundary(psql_conn, schema, input, output, reference, temp, bounds)

    print("Excess_Area :-", excess_area_without_parameters(psql_conn, schema ,output,reference),
              "Distortion :-", get_distortion(psql_conn, schema, output, input))
    

def fit_with_excess_area(psql_conn, schema, input, output, reference, temp, bounds):
    create_union(psql_conn, schema, input, input+"_union")
    result = minimize(excess_area, [0, 0, 0, 0, 0],args=(psql_conn, schema, input+"_union",reference, temp),bounds=bounds)
    print(f"Resulting Transformation Parameters {result.x}")
    update_rotation(psql_conn, schema, input, output, result.x[0])
    update_scale(psql_conn, schema, output, temp, result.x[1], result.x[2])
    update_translation(psql_conn, schema, temp, output, result.x[3], result.x[4])


def fit_with_excess_area_at_boundary(psql_conn, schema, input, output, reference, temp, bounds):
    create_union(psql_conn, schema, input, input+"_union")
    farmplots_clipped = "farmplots_clipped"
    clip_farmplots(psql_conn, schema, reference, farmplots_clipped, input+"_union")
    result = minimize(excess_area_at_boundary, [0, 1, 1, 0, 0], args=(psql_conn, schema, input+"_union", farmplots_clipped),bounds=bounds)
    print(f"Resulting Transformation Parameters: {result.x}")
    update_rotation(psql_conn, schema, input, output, result.x[0])
    update_scale(psql_conn, schema, output, temp, result.x[1], result.x[2])
    update_translation(psql_conn, schema, temp, output, result.x[3], result.x[4])


def create_union(psql_conn, schema, input, output):
    sql = f'''
            drop table if exists {schema + "." + output};
            create table {schema + "." + output} as 
            select 
                st_MakePolygon(st_exteriorRing(st_union(geom))) as geom from {schema + "." + input};
        '''
    with psql_conn.connection().cursor() as curr:
            curr.execute(sql)


def excess_area(parameters, psql_conn, schema, input, reference, temporary):
    input_table = schema + "." + input
    temporary_table = schema + "." + temporary
    sql = f'''
            drop table if exists {temporary_table};
            create table {temporary_table} as
            with 
            center as (
                select st_centroid(
                    st_MakePolygon(
                        st_exteriorRing(
                            st_union(geom)
                        )
                    )
                ) as geom from {input_table}
            ),    
            rotated as (
                select st_rotate(p.geom,{parameters[0]},c.geom) as geom from center as c, {input_table} as p
            ),
            scaled as (
                select st_scale(p.geom,st_makePoint({parameters[1]},{parameters[2]}),c.geom) as geom
                from center as c,rotated as p
            )
            
            select st_translate(geom, {parameters[3]}, {parameters[4]}) as geom from scaled;
        '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
    
    val = calculate_a_minus_b(psql_conn, schema, reference, temporary) + calculate_a_minus_b(psql_conn, schema, temporary, reference)
    return val



def calculate_a_minus_b(psql_conn, schema, a, b):
    sql = '''
            select 
            st_area(
                st_difference(
                    (select st_union(geom) from {table_a}),
                    (select st_union(geom) from {table_b})
                )
            );
        '''.format(table_a=schema + "." + a,
                   table_b=schema + "." + b)
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        val_fetch = curr.fetchall()

    if val_fetch is None:
        val=0
    elif val_fetch[0][0] is None:
        val=0
    else:
        val=val_fetch[0][0]
        
    val = float(val)
        
    return val


def update_rotation(psql_conn, schema, input, output, r):
    input_table = schema + "."+ input
    output_table = schema + "." + output
    sql = f'''
        drop table if exists {output_table};
        create table {output_table} as table {input_table};
        with center as (
            select st_centroid(st_union(geom)) as geom from {input_table}
        )
           
        update {output_table}
        set geom = (select st_rotate({output_table}.geom, {r}, c.geom) from center as c);
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)

def update_scale(psql_conn, schema, input, output, x, y):
    input_table = schema+ "."+input
    output_table = schema+ "."+output
    sql = f'''
        drop table if exists {output_table};
        create table {output_table} as table {input_table};
        with center as (
            select st_centroid(st_union(geom)) as geom from {input_table}
        )
            
        update {output_table}
        set geom = (select st_scale({output_table}.geom, st_makepoint({x},{y}), c.geom) from center as c);
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)    


def update_translation(psql_conn, schema, input, output, x, y):
    input_table = schema + "." + input
    output_table = schema + "." + output
    sql = f'''
        drop table if exists {output_table};
        create table {output_table} as table {input_table};

        update {output_table}
        set geom = st_translate(geom, {x}, {y});
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)


def clip_farmplots(psql_conn, schema, input, output, reference):
    input_table = schema + "." + input
    output_table = schema + "." + output
    reference_table = schema + "." + reference
    sql = f'''
        drop table if exists {output_table};
        create table {output_table} as select * from {input_table} as f
        where 
        st_length(
            st_shortestline(
                (select 
                    st_transform(
                        st_boundary(geom),32643) 
                from {reference_table}),
                st_transform(f.geom,32643))
            ) <= 150;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)


def excess_area_at_boundary(parameters, psql_conn, schema,   input, reference , ref_schema = None):
    if ref_schema is None:
        ref_schema = schema
    input_table = schema + "." + input
    reference_table = ref_schema + "." + reference    
    sql = f''' 
        with center as (
        select st_centroid(
                st_MakePolygon(
                    st_exteriorRing(
                        st_union(geom)
                    )
                )
        )
         as geom from {input_table})
        , rotated as (
            select st_rotate(p.geom,{parameters[0]},c.geom) as geom from center as c, {input_table} as p
        )
        , scaled as (
            select st_scale(p.geom,st_makePoint({parameters[1]},{parameters[2]}),c.geom) as geom from center as c,
            rotated as p
        )
        select sum(
            least(
                st_area(
                    st_difference(
                        st_transform(Q.geom,32643),
                        st_transform((select st_union(st_translate(geom,{parameters[3]},{parameters[4]})) from scaled),32643)
                    )
                ),
                st_area(
                    st_intersection(
                        st_transform(Q.geom,32643),
                        st_transform((select st_union(st_translate(geom,{parameters[3]},{parameters[4]})) from scaled),32643)
                    )
                )
            )
        )
                    
        from 
            {reference_table} as Q
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        a = curr.fetchall()[0][0]
    if (a == None or not a):
        print("None in excess area")
        return 0
    else:
        return float(str(a))
    

def excess_area_without_parameters(psql_conn, schema, input, reference, ref_schema = None):
    if ref_schema is None:
        ref_schema = schema
    input_table = schema + "." + input
    reference_table = ref_schema + "." + reference
    sql = f'''
    select sum(
            least(
                st_area(
                    st_difference(
                        st_transform(Q.geom,32643),
                        (select st_union(st_makevalid(geom)) from {input_table})
                    )
                ),
                st_area(
                    st_intersection(
                        st_transform(Q.geom,32643),
                        (select st_union(st_makevalid(geom)) from {input_table})
                    )
                )
            )
        )
                
        from 
            {reference_table} as Q
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        a = curr.fetchall()[0][0]
    if (a == None or not a):
        print("None in excess area")
        return 0
    else:
        return float(str(a))
    

def get_distortion(psql_conn, schema, input, reference, ref_schema= None):
    if ref_schema is None:
        ref_schema = schema
    input_table = schema + "." + input
    reference_table = ref_schema + "." + reference
    sql = f'''
        select
            stddev(abs(st_area(a.geom)/st_area(b.geom)))
        from 
            {input_table} as a,
            {reference_table} as b
        where
            a.gid = b.gid
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        std_dev = curr.fetchall()
    return std_dev[0][0]

def update_covered_area(psql_conn, schema):
    sql_query=f"""
        
        insert into {schema}.covered_area
        select geom as geom from {schema}.mapped_polygon;

    
        drop table if exists {schema}.temp_covered_area;
        create table {schema}.temp_covered_area as

        select st_union(geom) as geom from {schema}.covered_area;

        drop table if exists {schema}.covered_area;
        create table {schema}.covered_area as 
        
        select st_union(geom) as geom from {schema}.temp_covered_area;
        
        insert into {schema}.covered_edge_ids
        select a.edge_id 
        from {schema}.temp_polygon_edges as a
        where NOT EXISTS (
            SELECT 1
            FROM {schema}.covered_edge_ids as b
            WHERE b.edge_id = a.edge_id
        );
        
    """

    with psql_conn.connection().cursor() as curs:
        curs.execute(sql_query)
    psql_conn.connection().commit()
    
def update_covered_edges(psql_conn, schema, cadastral_topo):
    cadastral_topo_schema = f'{schema}{cadastral_topo}'
    sql_query=f"""
        with current_nodes as (
            select 
                node_id,
                geom
            from
                {schema}.best_mapped_nodes
        ),
        nodes as (
            select
                node_id,
                geom
            from 
                {schema}.shifted_nodes
        ),
        covered as (
            select
                st_union(geom) as geom
            from
                {schema}.covered_area
        ),
        other_edges as (
            select 
                edges.edge_id as edge_id,
                st_makeline(new_nodes.geom,nodes.geom) as geom  
            from 
                {cadastral_topo_schema}.edge as edges,
                nodes as nodes,
                current_nodes as new_nodes
            where
                ((
                    edges.start_node = nodes.node_id
                    and
                    edges.end_node = new_nodes.node_id
                )
                or
                (
                    edges.end_node = nodes.node_id
                    and
                    edges.start_node = new_nodes.node_id 
                ))
                and
                (edges.edge_id not in (select a.edge_id from {schema}.covered_edge_ids as a))                    
        ),
        inserting_ids as (
            insert into {schema}.covered_edge_ids
            select a.edge_id from other_edges as a
            returning edge_id
        )
        
        insert into {schema}.covered_edges
        select st_union(geom) as geom from other_edges;
        
        drop table if exists {schema}.temp_covered_edges;
        create table {schema}.temp_covered_edges as

        select geom from {schema}.covered_edges;


        drop table if exists {schema}.covered_edges;
        create table {schema}.covered_edges as
        select st_union(geom) as geom from {schema}.temp_covered_edges;
    """

    with psql_conn.connection().cursor() as curs:
        curs.execute(sql_query)
    psql_conn.connection().commit()
