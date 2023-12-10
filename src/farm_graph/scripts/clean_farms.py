from config import *
from utils import *
import argparse

def farmplot_cleaner(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Farmplot_Cleaner(config,pgconn)

class Farmplot_Cleaner:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.farmplots_dup = self.config.setup_details['data']['farmplots_temp_table']
        if self.village == "":
            print("ERROR")
            exit()
            
    def remove_trees(self, table):
        sql = f'''
            delete from {table} 
            where 
                description != 'field'
            and
                description != 'orchard';
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def dedup_geom(self, village, input, output):
        sql_query=f"""

            drop table if exists {village}.{output};
            create table {village}.{output} as

            select
                gid,
                st_multi(geom) as geom,
                description
            from
                {village}.{input}
            ;


            with cp as (
                select gid, geom from {village}.{output}
            ),
            duplicate_geom as (
                select
                    cp2.gid
                from
                    cp cp1,
                    cp cp2
                where
                    st_intersects(cp1.geom, cp2.geom)
                    and (st_area(st_intersection(cp1.geom, cp2.geom))/st_area(cp1.geom)>0.5
                        or st_area(st_intersection(cp1.geom, cp2.geom))/st_area(cp2.geom)>0.5)
                    and cp1.gid!=cp2.gid
                    and (st_area(cp1.geom)>st_area(cp2.geom)
                        or (st_area(cp1.geom)=st_area(cp2.geom) and cp1.gid>cp2.gid))
                order by
                    cp2.gid
            )

            delete from {village}.{output} dedup
            using duplicate_geom
            where dedup.gid=duplicate_geom.gid
            ;

            with cp as (
                select gid, geom from {village}.{output}
            ),
            clipped_cp as (
                select
                    cp1.gid as gid,
                    cp1.geom as original_geom,
                    st_difference(cp1.geom,
                        st_union(st_buffer(cp2.geom, 1, 'join=mitre'))
                    ) as new_geom
                from
                    cp cp1,
                    cp cp2
                where
                    st_intersects(cp1.geom, cp2.geom)
                    and
                    cp1.gid < cp2.gid
                group by
                    cp1.gid,
                    cp1.geom
            )
            
            update {village}.{output} farmplots
            set geom = st_multi(clipped_cp.new_geom)
            from clipped_cp
            where farmplots.gid = clipped_cp.gid
            ;
        """

        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    def run(self):
        update_srid(self.psql_conn, self.village+'.'+self.farmplots_dup, 'geom', 32643)
        add_column(self.psql_conn, self.village, self.farmplots_dup, 'gid','serial')
        rename_column(self.psql_conn,  self.village, self.farmplots_dup, 'descriptio','description')
        self.remove_trees(self.village+'.'+self.farmplots_dup)
        self.dedup_geom(self.village, self.farmplots_dup, self.farmplots)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="deolanabk")

    argument = parser.parse_args()
    
    village = argument.village

    datacleaner = farmplot_cleaner(village)
    datacleaner.run()

