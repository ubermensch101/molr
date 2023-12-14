from config import *
from utils import *
import argparse

def assign_type(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Type_Assigner(config,pgconn)

class Type_Assigner:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.ownership_polygons = self.config.setup_details['pos']['ownership']
        self.shifted_faces = self.config.setup_details['pos']['shifted_faces']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']
        self.farmplots = self.config.setup_details['pos']['farmplots']
        if self.village == "":
            print("ERROR")
            exit()

    def assign_type_farm_vs_void(self):
        """Assgins type as farm or void and calls the create super_poly function that assigns ownership 
        and takes union of farm faces"""
        
        sql_query = f'''
        
            drop table if exists {self.village}.{self.ownership_polygons};
            create table {self.village}.{self.ownership_polygons} as
            with filter as 
            (
                select st_union(geom) as geom from {self.village}.{self.shifted_faces}
            )
            select * from {self.village}.{self.farm_faces}
            where 
                st_area(geom)>1
                and
                st_intersects(
                    geom,
                    (select f.geom from filter as f)
                )
            ;
            
            delete from {self.village}.{self.ownership_polygons}
            where st_area(geom)<1;

            -- type can be void, farm
            alter table {self.village}.{self.ownership_polygons}
            add column type varchar;

            update {self.village}.{self.ownership_polygons} as p
            set type = 
            CASE
                WHEN 
                    st_area(
                        st_intersection(
                            p.geom,(
                                select 
                                st_union(geom) 
                                from {self.village}.{self.farmplots}
                            )
                        )
                    ) < 0.1*st_area(geom)
                THEN 'void'
                ELSE 'farm'
            END;    
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    def run(self):
        self.assign_type_farm_vs_void()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    typeassigner = assign_type(village)
    typeassigner.run()
