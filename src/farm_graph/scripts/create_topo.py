from config import *
from utils import *
import argparse

def farmplot_topo_creator(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Farmplot_Topo_Creator(config,pgconn)

class Farmplot_Topo_Creator:
    def __init__(self, config: Config, psql_conn: PGConn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        
        self.fp_midlines = self.config.setup_details['fp']['fp_midlines_table']
        self.fp_midlines_edges = self.config.setup_details['fp']['fp_midlines_edges_table']
        self.fp_voids = self.config.setup_details['fp']['fp_voids_table']
        self.fp_void_polygons = self.config.setup_details['fp']['fp_void_polygons_table']
        self.fp_mid_poly = self.config.setup_details['fp']['fp_midline_polygons']
        self.fp_mid_poly_filtered = self.config.setup_details['fp']['fp_midline_polygons_filtered']
        
        self.temp_farm_topo = self.village+self.config.setup_details['fp']['farm_topo_temp_suffix']
        self.farm_topo = self.village+config.setup_details['fp']['farm_topo_suffix']
        
        self.area_thresh = self.config.setup_details['fp']['valid_fp_area_threshold']
        self.intersection_thresh = self.config.setup_details['fp']['filtering_intersection_threshold']
        self.seg_tol = self.config.setup_details['fp']['seg_tol']
        self.seg_length = self.config.setup_details['fp']['seg_length']
        self.tol = self.config.setup_details['fp']['topo_tol']

        if self.village == "":
            print("ERROR")
            exit() 
            
    def filter_polygons(self):
        input = self.village+'.'+self.fp_mid_poly
        output = self.village+'.'+self.fp_mid_poly_filtered
        farmplots = self.village+'.'+self.farmplots
        
        sql_query=f"""
            drop table if exists {output};
            create table {output} as

            select
                st_multi((st_dump(
                    polygons.geom
                )).geom) as geom
            from
                {input} polygons,
                {farmplots} cp
            where
                st_intersects(polygons.geom, cp.geom)
                and
                st_area(st_intersection(polygons.geom, cp.geom))/st_area(cp.geom) > {self.intersection_thresh}
                and
                st_area(cp.geom) > {self.area_thresh}
            ;
            
            alter table {output}
            add column gid serial;
        """
        
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            
            
    def segmentize(self):
        sql_query=f"""
            with edges as (
                select edge_id, start_node, end_node, geom from {self.farm_topo}.edge_data
            ),
            boundary as (
                select
                    (st_dumppoints(st_segmentize(geom, {self.seg_length}))).geom as point
                from
                    edges
            )
            
            select topogeo_addpoint('{self.farm_topo}', point, {self.seg_tol}) from boundary;
        """
        
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    def run(self):
        create_topo(self.psql_conn, self.village, self.temp_farm_topo, self.fp_midlines_edges, self.tol)
        polygonize_topo(self.psql_conn, self.village, self.temp_farm_topo, self.fp_mid_poly)
        self.filter_polygons()
        create_topo(self.psql_conn, self.village, self.farm_topo, self.fp_mid_poly_filtered, self.tol)
        self.segmentize()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    midlines = farmplot_topo_creator(village)
    midlines.run()

