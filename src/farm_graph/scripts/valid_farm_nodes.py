from config import *
from utils import *
import argparse

def create_valid_farm_nodes(config,psql_conn):
    village = config.setup_details['setup']['village']
    vfn_table = village+"."+config.setup_details['fp']['valid_farm_nodes_table']
    farm_schema = village+config.setup_details['fp']['farm_topo_suffix']
    
    sql_query=f"""
        drop table if exists {vfn_table};
        create table {vfn_table} as

        with neigh as (
            select
                count(edge.edge_id) as count,
                node.node_id as node_id
            from
                {farm_schema}.edge as edge,
                {farm_schema}.node as node
            where
                start_node = node_id
                or end_node = node_id
            group by
                node_id
        )

        select
            r.node_id as node_id,
            r.geom as geom,
            neigh.count as neigh_count
        from
            {farm_schema}.node as r,
            neigh
        where
            r.node_id = neigh.node_id
            and
            neigh.count > 2
        ;


        insert into {vfn_table}

        with neigh as (
            select
                count(edge.edge_id) as count,
                node.node_id as node_id
            from
                {farm_schema}.edge as edge,
                {farm_schema}.node as node
            where
                start_node = node_id
                or end_node = node_id
            group by
                node_id
        ),
        rel_edges as (
            select
                p.edge_id as e1,
                q.edge_id as e2,
                p.start_node as e1_start,
                p.end_node as e1_end,
                q.start_node as e2_start,
                q.end_node as e2_end,
                p.geom as e1_geom,
                q.geom as e2_geom
            from
                {farm_schema}.edge as p,
                {farm_schema}.edge as q
            where
                (
                    p.start_node=q.end_node or p.end_node=q.start_node
                )
                and
                    p.edge_id < q.edge_id
                and
                (
                    (degrees(st_angle(p.geom,q.geom)) > 45
                        and degrees(st_angle(p.geom,q.geom)) < 135)
                    or
                    (degrees(st_angle(p.geom,q.geom)) > 235
                        and degrees(st_angle(p.geom,q.geom)) < 315)
                )
        )

        select
            r.node_id as node_id,
            r.geom as geom,
            neigh.count as neigh_count
        from
            {farm_schema}.node as r,
            neigh,
            rel_edges
        where
            neigh.node_id=r.node_id
            and
            neigh.count=2
            and
            (
                (neigh.node_id=rel_edges.e1_start
                and
                neigh.node_id=rel_edges.e2_end)
                or
                (neigh.node_id=rel_edges.e1_end
                and
                neigh.node_id=rel_edges.e2_start)
            )
        ;
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql_query)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    create_valid_farm_nodes(config, pgconn)
