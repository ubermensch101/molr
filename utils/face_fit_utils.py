from .postgres_utils import *
        
def create_nodes_table(psql_conn, schema, table, srid = 32643):
    sql = f"""
        drop table if exists {schema}.{table};
        create table {schema}.{table} (
            node_id integer primary key,
            geom geometry(Point, {srid})
        );
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
    
def create_edges_table(psql_conn, schema, table, srid = 32643):
    sql = f"""
        drop table if exists {schema}.{table};
        create table {schema}.{table} (
            edge_id integer primary key,
            geom geometry(Linestring, {srid})
        );
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
    
def create_faces_table(psql_conn, schema, table, srid = 32643):
    sql = f"""
        drop table if exists {schema}.{table};
        create table {schema}.{table} (
            face_id integer,
            geom geometry(Polygon, {srid})
        );
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def create_face_node_map(psql_conn, schema, table, topo):
    sql = f"""
        drop table if exists {schema}.{table};
        create table {schema}.{table} as
        
        select
            f.face_id,
            n.node_id
        from
            {topo}.face as f,
            {topo}.node as n
        where
            f.face_id != 0
            and
            st_intersects(st_getfacegeometry('{topo}',f.face_id), n.geom)
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
    
def update_covered_nodes(psql_conn, schema, covered_nodes, input_table, input_table_id_column='node_id',
                 input_table_geom_column='geom'):
    sql = f"""
        with inserted_rows as (
            insert into {schema}.{covered_nodes} (node_id, geom)
            select
                {input_table_id_column},
                {input_table_geom_column}
            from
                {schema}.{input_table}
            on conflict do nothing 
            returning node_id, geom
        ),
        non_inserted_rows as (
            select
                inp.{input_table_id_column} as node_id,
                inp.{input_table_geom_column} as geom
            from
                {schema}.{input_table} as inp
            left join 
                inserted_rows as ir 
                on inp.{input_table_id_column} = ir.node_id
            where 
                ir.node_id is null
        ) 
        select
            nir.node_id,
            case 
                when st_equals(nir.geom, nodes.geom) then 'skipped' 
                else 'inconsistent' 
            end
        from
            non_inserted_rows as  nir
        left join
            {schema}.{covered_nodes} as nodes
            on nir.node_id = nodes.node_id
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        common_nodes = curr.fetchall()
    
    inconsistent = False
    
    for node_id, label in common_nodes:
        if label=='skipped':
            print(f"Node_id {node_id} already there with same geom, skipping")
        else:
            inconsistent=True
            print(f"ERROR!!!! Node_id {node_id} already there with different geom, halting")    

    if inconsistent:
        exit()

def update_covered_edges(psql_conn, schema, covered_nodes, covered_edges, topo):
    sql = f"""
        with new_edges as (
            select
                e.edge_id as edge_id,
                st_makeline(start_points.geom, end_points.geom) as geom
            from 
                {topo}.edge_data as e
            join
                {schema}.{covered_nodes} as start_points
                on start_points.node_id = e.start_node
            join
                {schema}.{covered_nodes} as end_points
                on end_points.node_id = e.end_node
        ),
        inserted_rows as (
            insert into {schema}.{covered_edges} (edge_id, geom)
            select
                edge_id,
                geom
            from
                new_edges
            on conflict do nothing 
            returning edge_id, geom
        ),
        non_inserted_rows as (
            select
                inp.edge_id as edge_id,
                inp.geom as geom
            from
                new_edges as inp
            left join 
                inserted_rows as ir 
                on inp.edge_id = ir.edge_id
            where 
                ir.edge_id is null
        ) 
        select
            nir.edge_id,
            case 
                when st_equals(nir.geom, edges.geom) then 'skipped' 
                else 'inconsistent' 
            end
        from
            non_inserted_rows as  nir
        left join
            {schema}.{covered_edges} as edges
            on nir.edge_id = edges.edge_id
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        common_edges = curr.fetchall()
    
    inconsistent = False
    
    for edge_id, label in common_edges:
        if label=='skipped':
            print(f"Edge_id {edge_id} already there with same geom, skipping")
        else:
            inconsistent=True
            print(f"ERROR!!!! edge_id {edge_id} already there with different geom, halting")    

    if inconsistent:
        exit()
        
def update_covered_faces(psql_conn, schema, covered_faces, input_polygon_table,input_table_id_column='face_id',
                 input_table_geom_column='geom'):
    sql = f"""
        with new_faces as (
            select
                {input_table_id_column} as face_id,
                {input_table_geom_column} as geom
            from 
                {schema}.{input_polygon_table}
        ),
        inserted_rows as (
            insert into {schema}.{covered_faces} (face_id, geom)
            select
                face_id,
                geom
            from
                new_faces
            on conflict do nothing 
            returning face_id, geom
        ),
        non_inserted_rows as (
            select
                inp.face_id as face_id,
                inp.geom as geom
            from
                new_faces as inp
            left join 
                inserted_rows as ir 
                on inp.face_id = ir.face_id
            where 
                ir.face_id is null
        ) 
        select
            nir.face_id,
            case 
                when st_equals(nir.geom, faces.geom) then 'skipped' 
                else 'inconsistent' 
            end
        from
            non_inserted_rows as  nir
        left join
            {schema}.{covered_faces} as faces
            on nir.face_id = faces.face_id
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        common_faces = curr.fetchall()
    
    inconsistent = False
    
    for face_id, label in common_faces:
        if label=='skipped':
            print(f"Face_id {face_id} already there with same geom, skipping")
        else:
            inconsistent=True
            print(f"ERROR!!!! face_id {face_id} already there with different geom, halting")    

    if inconsistent:
        exit()
    
def commit_nodes(psql_conn, topo, schema, covered_nodes, covered_edges, covered_faces, 
                 input_table, input_table_id_column='node_id',
                 input_table_geom_column='geom',face_node_map=None):
    
    update_covered_nodes(psql_conn, schema, covered_nodes, input_table, input_table_id_column,
                 input_table_geom_column)
    update_covered_edges(psql_conn, schema, covered_nodes, covered_edges, topo)
    
    if face_node_map==None:
        face_node_map = schema + '.temp_face_node_map'
        create_face_node_map(psql_conn, schema, face_node_map, topo)
        
    sql = f"""
        with nodes_count as (
            select
                face_id as face_id,
                count(node_id) as count
            from
                {schema}.{face_node_map}
            group by
                face_id
        ),
        current_nodes_count as (
            select 
                mapping.face_id as face_id,
                count(nodes.node_id) as count
            from
                {schema}.{covered_nodes} as nodes
            join
                {schema}.{face_node_map} as mapping
                on mapping.node_id = nodes.node_id
            group by
                mapping.face_id
        )
        select
            a.face_id
        from
            nodes_count as a
        join
            current_nodes_count as b
            on a.face_id = b.face_id
        where
            a.count = b.count
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        faces_created = curr.fetchall()
    
    output_table = 'temp_polygon'    
    
    for face_id, _ in faces_created:
        print(f"Committing for face_id {face_id}. Polygon stored in table {schema}.{output_table}")
        a = input("Press Enter to continue : ")
    
        get_face(psql_conn, topo, face_id, schema, output_table ,input_table , input_table_id_column, input_table_geom_column)
        if not check_face_valid(psql_conn, face_id, schema, output_table, covered_faces, covered_edges):
            continue
        update_covered_faces(psql_conn, schema, covered_faces, output_table,'face_id','geom')

        print(f"Committed face_id {face_id}")
    
def commit_face(psql_conn, topo, schema, covered_nodes, covered_edges, covered_faces, face_id, mapped_polygon,
                 input_table, input_table_id_column='node_id',
                 input_table_geom_column='geom'):
    
    print(f"Committing for face_id {face_id}. Polygon stored in table {mapped_polygon}")
    a = input("Press Enter to continue : ")
    
    update_covered_nodes(psql_conn, schema, covered_nodes, input_table, input_table_id_column,
                 input_table_geom_column)
    update_covered_edges(psql_conn, schema, covered_nodes, covered_edges, topo)
    update_covered_faces(psql_conn, schema, covered_faces, mapped_polygon, 'face_id', 'geom')
    
    print(f"Committed face_id {face_id}")
    

def get_face(psql_conn, topo, face_id, schema, output_table, input_table, input_table_id_column='node_id',
                 input_table_geom_column='geom'):
    sql = f"""
        drop table if exists {schema}.{output_table};
        create table {schema}.{output_table} as
    
        with edges as (
            select 
                t.seq as seq,
                t.edge_id as edge_id
            from
                st_getfaceedges('{topo}',{face_id}) as t(seq, edge_id)
        ),
        new_edges as (
            select
                e.edge_id as edge_id,
                st_makeline(
                    start_nodes.{input_table_geom_column}, 
                    end_nodes.{input_table_geom_column}
                ) as geom
            from
                edges as e
            join
                {topo}.edge_data as ed
                on abs(ed.edge_id) = abs(e.edge_id)
            join
                {schema}.{input_table} as start_nodes
                on ed.start_node = start_nodes.{input_table_id_column}
            join
                {schema}.{input_table} as end_nodes
                on ed.end_node = end_nodes.{input_table_id_column}
        )
        
        select
            {face_id} as face_id,
            st_makevalid((st_dump(st_polygonize(geom))).geom) as geom
        from
            new_edges
        limit
            1
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def check_face_valid(psql_conn, face_id, schema, table, covered_faces, covered_edges):
    sql = f"""
        select count(*)=1 and st_isvalid(st_collect(geom)) as validity from {schema}.{table};
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        validity = curr.fetchone()
    
    if validity is None or not validity[0]:
        print("Table or geom not valid")
        return False
    
    sql = f"""
        with covered_area as (
            select
                st_collect(st_collect(f.geom),st_collect(e.geom)) as geom
            from
                {schema}.{covered_faces} as f,
                {schema}.{covered_edges} as e
        )
        select 
            coalesce(
                st_intersects(
                    st_buffer(
                        p.geom,-0.02, 'join=mitre'
                    ), 
                    c.geom
                ),
                false
            )
        from
            {schema}.{table} as p,
            covered_area as c
        limit
            1
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        validity = curr.fetchone()
    
    if validity is None or validity[0]:
        print("Lies in covered area")
        return False

    return True

def average_translate_face_nodes(psql_conn, schema, topo_schema,
                                 face_id, face_node_map, covered_nodes, output_nodes_table,
                                 nodes_geom_table=None):
    if nodes_geom_table==None:
        sql = f"""
            drop table if exists {schema}.{output_nodes_table};
            create table {schema}.{output_nodes_table} as
            
            with nodes as (
                select
                    node_id
                from
                    {schema}.{face_node_map}
                where
                    face_id = {face_id}
            ),
            geom_nodes as (
                select
                    n.node_id as node_id,
                    cn.geom as shifted_geom,
                    tn.geom as original_geom
                from
                    nodes as n
                left join
                    {schema}.{covered_nodes} as cn
                    on n.node_id = cn.node_id
                left join
                    {topo_schema}.node as tn
                    on n.node_id = tn.node_id
            ),
            average_translate as (
                select
                    coalesce(avg(st_x(shifted_geom)-st_x(original_geom)),0) as delta_x,
                    coalesce(avg(st_y(shifted_geom)-st_y(original_geom)),0) as delta_y
                from
                    geom_nodes as n
            )
            select
                n.node_id as node_id,
                case
                    when n.shifted_geom is null 
                        then st_translate(n.original_geom, a.delta_x, a.delta_y)
                    else n.shifted_geom
                end as geom
            from
                average_translate as a,
                geom_nodes as n
            ;
        """
    else:
        sql = f"""
            drop table if exists {schema}.{output_nodes_table};
            create table {schema}.{output_nodes_table} as
            
            with average_translate as (
                select
                    coalesce(avg(st_x(shifted_geom)-st_x(original_geom)),0) as delta_x,
                    coalesce(avg(st_y(shifted_geom)-st_y(original_geom)),0) as delta_y
                from
                    {schema}.{nodes_geom_table} as n
            )
            select
                n.node_id as node_id,
                case
                    when n.shifted_geom is null 
                        then st_translate(n.original_geom, a.delta_x, a.delta_y)
                    else n.shifted_geom
                end as geom
            from
                average_translate as a,
                {schema}.{nodes_geom_table} as n
            ;
        """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
def get_nodes_geom(psql_conn, schema, topo_schema, temp_nodes_geom_table, 
                   face_node_map, covered_nodes, face_id):
    sql = f"""
        drop table if exists {schema}.{temp_nodes_geom_table};
        create table {schema}.{temp_nodes_geom_table} as 
        
        with nodes as (
            select
                node_id as node_id
            from
                {schema}.{face_node_map}
            where
                face_id = {face_id}
        )
        
        select
            n.node_id as node_id,
            cn.geom as shifted_geom,
            tn.geom as original_geom
        from
            nodes as n
        left join
            {schema}.{covered_nodes} as cn
            on n.node_id = cn.node_id
        left join
            {topo_schema}.node as tn
            on n.node_id = tn.node_id
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)