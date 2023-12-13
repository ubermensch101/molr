from config import *
from utils import *
import argparse

# wrapper
def cut_farm(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Cut_Farm(config,pgconn)

class Cut_Farm:

    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        # self.ownership_polygons = self.config.setup_details['pos']['ownership']
        self.shifted_faces = self.config.setup_details['pos']['shifted_faces']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']
        self.farmplots = self.config.setup_details['pos']['farmplots']
        self.void_percent_threshold =self.config.setup_details['pos']['void_percent_threshold']
        self.possession_1 = self.config.setup_details['pos']['possession_1']
        self.possession_2 = self.config.setup_details['pos']['possession_2']
        self.possession_3 = self.config.setup_details['pos']['possession_3']
        self.possession_4 = self.config.setup_details['pos']['possession_4']
        self.possession_5 = self.config.setup_details['pos']['possession_5']
        self.possession_final = self.config.setup_details['pos']['possession_final']
        self.ownership = self.config.setup_details['pos']['ownership']
        self.ownership_2 = self.config.setup_details['pos']['ownership_2']
        self.ownership_3 = self.config.setup_details['pos']['ownership_3']
        self.ownership_4 = self.config.setup_details['pos']['ownership_4']
        self.farm_superpoly = self.config.setup_details['pos']['farm_superpoly_topo']
        self.edge = self.config.setup_details['pos']['edge']
        self.topo = self.config.setup_details['pos']['topo']
    # "possession_1":"possession_1",
    # "possession_2":"possession_2",
    # "possession_3":"possession_3",
    # "possession_4":"possession_4",
    # "possession_5":"possession_5",
    # "ownership":"farm_ownership",
    # "ownership_2":"farm_ownership_2",
    # "ownership_3":"farm_ownership_3",
    # "ownership_4":"farm_ownership_4",
    # "farm_superpoly_topo":"_farm_superpoly"

        if self.village == "":
            print("ERROR")
            exit()

    def create_super_poly(self, input_table, output_table):

        """Input table -- farm graph polygons
        Output table --- super polygons table name(will be created in the function)"""
        # void_abs_threshold = 5000
        
        sql_query = f'''
            --survey_gid ->gid of corresponding survey plot
            alter table {input_table}
            add column if not exists survey_gid int;
            
            update {input_table} as p
            set survey_gid = 
            CASE
                WHEN type = 'farm'
                THEN
                (
                    select gid from {self.village}.{self.shifted_faces} as q
                    where 
                    (st_area(p.geom) < 10000 
                    and
                    st_area(
                        st_intersection(
                            p.geom, q.geom
                        )
                    ) > 0.7*st_area(p.geom))   
                    or  
                    (st_area(
                        st_intersection(
                            p.geom, q.geom
                        )
                    ) > 0.9*st_area(p.geom))
                    order by st_area(st_intersection(p.geom, q.geom)) desc
                    limit 1
                )
                ELSE
                (
                    select gid from {self.village}.{self.shifted_faces} as q
                    where 
                    st_area(st_intersection(p.geom, q.geom)) > {self.void_percent_threshold}*st_area(p.geom)

                    order by st_area(st_intersection(p.geom, q.geom)) desc
                    limit 1
                )
            END
            where survey_gid is NULL
            ;
            
            drop table if exists {output_table};
            create table {output_table} as 
            select 
                survey_gid,
                st_union(geom) as geom 
                from {input_table} 
                where survey_gid is not null
                group by survey_gid;
            
            alter table {output_table}
            add column gid serial;
        '''
        with self.pgconn.cursor() as curr:
            curr.execute(sql_query)

    def create_super_poly_2():
        pass

    def break_voids(self, input_onwership_polygons, topo_edges, output_ownership_polygons):
        """ Function to break unassigned farmplots and voids
        """
        
        sql_query = f'''
            drop table if exists {output_ownership_polygons};
            create table {output_ownership_polygons}
            as select * from {input_onwership_polygons};
            
            select 
                gid 
            from 
                {input_onwership_polygons}
            where
                survey_gid is NULL
                and 
                st_area(geom) > 10000    
            ;
        '''
        with self.pgconn.cursor() as curr:
            curr.execute(sql_query)
            res = curr.fetchall()
            
        for (gid,) in res:
            print(f"Processing {input_onwership_polygons} gid {gid}")
            edge_id = self.find_splitting_edge(gid, input_onwership_polygons, topo_edges)
            if edge_id == -1:
                continue
            print("chosen edge id",edge_id)
            
            sql_query = f'''
                drop table if exists {village}.temp;
                create table {village}.temp as
                select 
                    (st_dump(
                        st_split(
                            (select geom from {input_onwership_polygons} where gid = {gid}),
                            (
                                SELECT 
                                    ST_MakeLine(
                                        ST_TRANSLATE(a, sin(az1) * len, cos(az1) * len),
                                        ST_TRANSLATE(b, sin(az2) * len, cos(az2) * len)
                                    )
                                FROM 
                                (
                                    SELECT 
                                        a, 
                                        b, 
                                        ST_Azimuth(a,b) AS az1, 
                                        ST_Azimuth(b, a) AS az2, 
                                        ST_Distance(a,b) + 200 AS len
                                    FROM 
                                    (
                                        SELECT 
                                            ST_StartPoint(geom) AS a, 
                                            ST_EndPoint(geom) AS b
                                        from 
                                            {topo_edges} 
                                        where 
                                            edge_id = {edge_id}
                                    ) AS sub
                                ) AS sub2
                            )
                        )
                    )).geom as geom,
                    (select type from {input_onwership_polygons} where gid = {gid}) as type
            '''
            with self.pgconn.cursor() as curr:
                curr.execute(sql_query)
                
            sql_query = f'''
                insert into {output_ownership_polygons} (geom,type)
                    select geom,type from {village}.temp;

                delete from {output_ownership_polygons}
                where gid = {gid};
            '''
            with self.pgconn.cursor() as curr:
                curr.execute(sql_query)

        sql_query = f'''
            alter table {output_ownership_polygons}
            drop column gid;

            alter table {output_ownership_polygons}
            add column gid serial;
        '''
        with self.pgconn.cursor() as curr:
            curr.execute(sql_query)

    def break_voids_2():
        pass

    def find_splitting_edge():
        pass

    def run(self):
        possession_var_1 = f'{self.village}.{self.possession_1}'
        possession_var_2 = f'{self.village}.{self.possession_2}'
        possession_var_3 = f'{self.village}.{self.possession_3}'
        possession_var_4 = f'{self.village}.{self.possession_4}'
        possession_var_5 = f'{self.village}.{self.possession_5}'

        farm_possession_poly = f'{self.village}.{self.possession_final}'

        # farm_faces = self.farm_faces

        farm_plot_ownership = f'{self.village}.{self.ownership}'
        farm_plot_ownership_2 = f'{self.village}.{self.ownership_2}'
        farm_plot_ownership_3 = f'{self.village}.{self.ownership_3}'
        farm_plot_ownership_4 = f'{self.village}.{self.ownership_4}'

        farm_superpoly_topo = {self.village}+{self.farm_superpoly}
        topo_edges = f'{self.village}{self.topo}.{self.edge}'

        self.create_super_poly(farm_plot_ownership , possession_var_1)

        self.add_column(self.psql_conn,self.village,'gid','serial')

        self.break_voids()
        self.create_super_poly(farm_plot_ownership_2, topo_edges, possession_var_2)

        self.break_voids()
        self.create_super_poly(farm_plot_ownership_3, possession_var_3)

        self.break_voids_2()
        self.create_super_poly_2(farm_plot_ownership_4, possession_var_4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    farmcutter = cut_farm(village)
    farmcutter.run()
