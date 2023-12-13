from utils import *
from config import *
from scripts import *
import argparse

def possession():
    config = Config()
    
    pgconn = PGConn(config.setup_details["psql"])
    conn = pgconn.connection()
    
    return Possession(config,conn)

class Possession:
    def __init__(self,config,psql_conn):
        self.config = config
        self.village = self.config.setup_details['setup']['village']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']
        self.psql_conn = psql_conn
        
    def cut_farms(self):
        # create_super_poly(.....)
        # add_column(self.psql_conn,self.village,'gid','serial')
        # break_voids(...)
        # create_super_poly(....)
        # break_voids(...)
        # create_super_poly(...)
        # break_voids_2(...)
        # create_super_poly_2(..)

        pass
    
    def assigning_farm_vs_void(self):
        assigner = Type_Assigner(self.config, self.psql_conn)
        assigner.run()
    
    def run(self):
        self.add_column()
        
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
        with self.pgconn.cursor() as curr:
            curr.execute(sql_query)

        self.assigning_farm_vs_void()
        self.cut_farms()
        # create_topo()
        # simplify(...)
        # clean_snap_error(...)
        # create_topology_edges(...)
        # polygonise()
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()

    village = argument.village
    pos = possession(village)
    pos.run()
