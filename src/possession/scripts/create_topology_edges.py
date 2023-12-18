from scripts import *
from utils import *
from config import *
import argparse

def create_topology_edges(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Topology_Edges_Creater(config,pgconn)

class Topology_Edges_Creater:

    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.ownership_polygons = self.config.setup_details['pos']['ownership']
        self.shifted_faces = self.config.setup_details['pos']['shifted_faces']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']
        self.farmplots = self.config.setup_details['pos']['farmplots']
        self.possession_4_var = config.setup_details['pos']['possession_4']
        self.possession_5_var = config.setup_details['pos']['possession_5']
        self.farm_superpoly_topo = config.setup_details['pos']['farm_superpoly_topo']
        self.edge = config.setup_details['pos']['edge']
        self.create_topo_tolerance = config.setup_details['pos']['create_topo_tolerance']
        if self.village == "":
            print("ERROR")
            exit()

    def simplify(self):
        "Takes a space partition of polygons and applies st_simplify"
        input_table = f'{self.village}_{self.farm_superpoly_topo}.{self.edge}'
        output_table = f'{self.village}.{self.possession_5_var}'
        sql_query = f'''
                drop table if exists {output_table};
                create table {output_table} as 
                select 
                    (st_dump(
                        st_simplify(
                            st_collect(geom),1
                        )
                    )).geom as geom   
                from
                    {input_table}
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

        sql_query = f'''
            update {output_table}
            set geom = (
                select st_snap(
                    geom,
                    (select st_collect(geom) from {output_table}),
                    0.1
                )
            )
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    def clean_snap_error(self):
        input_edges = f'{self.village}.{self.possession_5_var}'
        sql_query = f'''
        update 
            {input_edges} 
        set geom = 
        (select 
            st_multi(
                st_snap(
                    geom,
                    (select st_collect(geom) from {input_edges}), 
                    1
                )
            )
        );
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    def create_topology_edges(self):
        input_table = f'{self.village}.{self.possession_5_var}'
        output_topo = f'{self.village}_{self.farm_superpoly_topo}'
        if check_schema_exists(self.psql_conn, output_topo):
            comment_cadastral_topo_drop=""
        else:
            comment_cadastral_topo_drop="--"
            
        sql_query=f"""
            {comment_cadastral_topo_drop}select DropTopology('{output_topo}');
            select CreateTopology('{output_topo}', 32643);

            select
                ((st_dump(st_force2d(geom))).geom) as geom
            from
                {input_table}
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            geoms_fetch=curr.fetchall()

        polygon_geoms=[geom_fetch[0] for geom_fetch in geoms_fetch]

        for polygon_geom in polygon_geoms:
            sql_query=f"""
                select TopoGeo_AddLineString(
                    '{output_topo}',
                    '{polygon_geom}'::geometry(Linestring, 32643)--,
                    --0.1
                );
            """  
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql_query)

    def run(self):
        output_topo = f'{self.village}_{self.farm_superpoly_topo}'
        create_topo(self.psql_conn, self.village, output_topo, self.possession_4_var, self.create_topo_tolerance)
        self.simplify()
        self.clean_snap_error()
        self.create_topology_edges()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    edges_creater = create_topology_edges(village)
    edges_creater.run()