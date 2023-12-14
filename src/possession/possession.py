from scripts import *
from utils import *
from config import *

import argparse

def possession(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Possession(config,pgconn)

    # config = Config()
    
    # pgconn = PGConn(config)
    # if village != "":    
    #     config.setup_details['setup']['village'] = village
    
    # return DataValidationAndPreparation(config,pgconn)

class Possession:
    def __init__(self,config,psql_conn):
        self.config = config
        self.village = self.config.setup_details['setup']['village']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']
        self.shifted_faces = self.config.setup_details['pos']['shifted_faces']
        self.farm_topo = self.config.setup_details['pos']['farm_superpoly_topo']
        self.psql_conn = psql_conn

        self.farm_ownership = self.config.setup_details['pos']['ownership']
        self.farm_ownership_2 = self.config.setup_details['pos']['ownership_2']
        self.farm_ownership_3 = self.config.setup_details['pos']['ownership_3']
        self.farm_ownership_4 = self.config.setup_details['pos']['ownership_4']

        self.possession_1 = self.config.setup_details['pos']['possession_1']
        self.possession_2 = self.config.setup_details['pos']['possession_2']
        self.possession_3 = self.config.setup_details['pos']['possession_3']
        self.possession_4 = self.config.setup_details['pos']['possession_4']
        self.possession_5 = self.config.setup_details['pos']['possession_5']

        self.possession_final = self.config.setup_details['pos']['possession_final']
        self.shifted_topo = self.config.setup_details['pos']['topo']
        
    def cut_farms(self):
        possession_1 = f'{self.village}.{self.possession_1}'
        possession_2 = f'{self.village}.{self.possession_2}'
        possession_3 = f'{self.village}.{self.possession_3}'
        possession_4 = f'{self.village}.{self.possession_4}'
        # possession_5 = f'{self.village}.{self.possession_5}'

        farm_plot_ownership = f'{self.village}.{self.farm_ownership}'
        farm_plot_ownership_2 = f'{self.village}.{self.farm_ownership_2}'
        farm_plot_ownership_3 = f'{self.village}.{self.farm_ownership_3}'
        farm_plot_ownership_4 = f'{self.village}.{self.farm_ownership_4}'

        # farm_possession_poly = f'{self.village}.{self.possession_final}'

        create_super_poly(self.config, self.psql_conn, farm_plot_ownership, possession_1)
        add_column(self.psql_conn, farm_plot_ownership, 'gid', 'serial')
        break_voids(self.config, self.psql_conn, farm_plot_ownership, farm_plot_ownership_2)
        create_super_poly(self.config, self.psql_conn, farm_plot_ownership_2, possession_2)
        break_voids(self.config, self.psql_conn, farm_plot_ownership_2, farm_plot_ownership_3)
        create_super_poly(self.config, self.psql_conn, farm_plot_ownership_3, possession_3)
        break_voids_2(self.config, self.psql_conn, farm_plot_ownership_3, farm_plot_ownership_4)
        create_super_poly_2(self.config, self.psql_conn, farm_plot_ownership_4, possession_4)
        self.create_final_possession()

    def create_final_possession(self):
        edges_creater = Topology_Edges_Creater(self.config, self.psql_conn)
        edges_creater.run()
    
    def assigning_farm_vs_void(self):
        assigner = Type_Assigner(self.config, self.psql_conn)
        assigner.run()
    
    def run(self):
        farm_faces = f'{self.village}.{self.farm_faces}'
        shifted_faces = f'{self.village}.{self.shifted_faces}'
        add_column(self.psql_conn, shifted_faces, 'gid', 'serial')
        
        sql_query = f"""
            with un as (
                select st_union(geom) as geom from {self.village}.{self.farm_faces}
            ),
            g as (
                select st_difference(st_envelope(un.geom),un.geom) as geom from un
            )
            
            insert into {self.village}.{self.farm_faces} (geom)
            select geom from g;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

        add_column(self.psql_conn, farm_faces, 'gid', 'serial')

        self.assigning_farm_vs_void()
        self.cut_farms()
        topo_name = f'{self.village}_{self.shifted_topo}'
        # polygonize_topo(self.psql_conn, self.village, topo_name, self.possession_final) # this was original

        # polygonize(f'{farm_superpoly_topo}.edge',farm_possession_poly)
        farm_superpoly_topo = f'{self.village}_{self.farm_topo}.edge'
        polygonize(self.psql_conn, farm_superpoly_topo, self.possession_final)
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()

    village = argument.village
    pos = possession(village)
    pos.run()
