from config import *
from utils import *
import argparse

def midline_creator(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Midline_Creator(config,pgconn)

class Midline_Creator:
    def __init__(self, config: Config, psql_conn: PGConn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        
        self.fp_midlines = self.config.setup_details['fp']['fp_midlines_table']
        self.fp_midlines_edges = self.config.setup_details['fp']['fp_midlines_edges_table']
        self.fp_voids = self.config.setup_details['fp']['fp_voids_table']
        self.fp_void_polygons = self.config.setup_details['fp']['fp_void_polygons_table']
        
        self.area_thresh = self.config.setup_details['fp']['valid_fp_area_threshold']
        self.ext_buf_1 = self.config.setup_details['fp']['fp_extension_buffer_1']
        self.ext_buf_2 = self.config.setup_details['fp']['fp_extension_buffer_2']
        self.void_ext_buf = self.config.setup_details['fp']['void_extension_buffer']
        self.midlines_edges_length_thresh = self.config.setup_details['fp']['edge_length_threshold']
        
        self.partition_table = self.config.setup_details['fp']['partition_table']
        self.partitioned_voids = self.config.setup_details['fp']['fp_voids_partitioned']
        self.partition_side_length = self.config.setup_details['fp']['partition_side_length']
        self.partition_ext_buf_1 = self.config.setup_details['fp']['partition_buffer_1']
        self.partition_ext_buf_2 = self.config.setup_details['fp']['partition_buffer_2']

        if self.village == "":
            print("ERROR")
            exit()
            
    def create_voids(self):
        
        farmplots = self.village+"."+self.farmplots
        voids = self.village+"."+self.fp_voids
        fp_void_polygons = self.village+"."+self.fp_void_polygons
        
        sql_query = f"""
            drop table if exists {voids};
            create table {voids} as
            
            with bounds as 
            (
                select
                    st_envelope(st_union(geom)) as geom
                from
                    {farmplots}
            )

            select st_union(st_difference(
                st_difference(
                    (select
                        geom
                    from
                        bounds
                    ),
                    (select
                        st_union(geom) as geom
                    from
                        {farmplots}
                    where
                        st_area(geom)>{self.area_thresh}
                    )
                ),
                st_difference(
                    (select
                        geom
                    from
                        bounds
                    ),
                    (select
                        st_union(st_buffer(geom,{self.ext_buf_1},'join=mitre')) as geom
                    from
                        {farmplots}
                    where
                        st_area(geom)>{self.area_thresh}
                    )
                )
            )) as geom;
            
            drop table if exists {fp_void_polygons};
            create table {fp_void_polygons} as 
            with un as (
                select 
                    st_union(geom) as geom 
                from 
                    {voids}
            ),
            fp as (
                select 
                    st_union(geom) as geom 
                from 
                    {farmplots} 
                where 
                    st_area(geom)>{self.area_thresh}
            )
            select 
                st_difference(
                    st_difference(
                        st_envelope(un.geom),
                        un.geom
                    ),
                    fp.geom
                ) as geom 
            from 
                un, fp;
            
            with fp as (
                select 
                    st_buffer(st_union(geom),{self.ext_buf_2},'join=mitre') as geom 
                from 
                    {farmplots} 
                where 
                    st_area(geom)>{self.area_thresh}
            ),
            bf as (
                select 
                    st_difference(
                        st_buffer(geom,{self.void_ext_buf},'join=mitre'),
                        (select geom from fp)
                    ) as geom 
                from 
                    {fp_void_polygons}
            )            
            update {voids}
            set geom = st_difference(geom,(select geom from bf));
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
    
    def partition_voids(self):
        
        voids = self.village+"."+self.fp_voids
        partitions = self.village+"."+self.partition_table
        voids_partition = self.village+"."+self.partitioned_voids
        
        sql_query = f"""
            drop table if exists {partitions};
            create table {partitions} as
            
            select 
                geom 
            from 
                (select 
                    (st_squaregrid(
                        {self.partition_side_length},
                        st_envelope(
                            st_union(b.geom)
                        )
                    )
                ).geom as geom
                from 
                    {voids} b
                ) as foo;
            
            alter table {partitions}
            add column face_id serial;

            drop table if exists {voids_partition};
            create table {voids_partition} as

            select
                partitions.face_id as face_id,
                st_intersection(
                    farmplots_void.geom,
                    st_buffer(partitions.geom,{self.partition_ext_buf_1},'join=mitre')
                ) as geom
            from
                {voids} farmplots_void,
                {partitions} partitions
            ;
            
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            
    def midlines(self):
        fp_midlines = self.village+"."+self.fp_midlines
        partitions = self.village+"."+self.partition_table
        voids_partition = self.village+"."+self.partitioned_voids
        fp_midlines_edges = self.village+"."+self.fp_midlines_edges
        
        sql_query = f"""
            drop table if exists {fp_midlines};
            create table {fp_midlines} (
                face_id int,
                geom geometry
            );

            select face_id, geom from {partitions};
        """

        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            faces_fetched=curr.fetchall()
        
        print("Total Faces :-",len(faces_fetched))
        
        for (face_id, geom) in faces_fetched:
            print("Creating midlines for face_id:", face_id)
            sql_query=f"""
                insert into {fp_midlines}
                
                with farmplots_partitioned as (
                    select
                        face_id,
                        st_buffer(geom,0.01,'join=mitre') as geom
                    from
                        {voids_partition}
                    where
                        face_id = {face_id}
                ),
                medial_axis as (
                    select
                        face_id,
                        st_approximatemedialaxis(geom) as geom
                    from
                        farmplots_partitioned
                )

                select
                    medial_axis.face_id as face_id,
                    st_intersection(
                        medial_axis.geom,
                        st_buffer(p.geom,{self.partition_ext_buf_2},'join=mitre')
                    ) as geom
                from
                    medial_axis,
                    {partitions} as p
                where
                    medial_axis.face_id = p.face_id
                ;
            """
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql_query)
        
        sql_query = f"""drop table if exists {fp_midlines_edges};
            create table {fp_midlines_edges} as 
            
            with noded as 
            (
                select st_union(geom) as geom
                from {fp_midlines}
            )
            ,
            edges as 
            (
                select 
                    (st_dump(geom)).geom as geom
                from 
                    noded
            )
            select
                geom as geom
            from
                edges
            where 
                st_length(geom) > {self.midlines_edges_length_thresh}
            ;
            
            alter table {fp_midlines_edges}
            add column gid serial;
            
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
    

    def run(self):
        self.create_voids()
        self.partition_voids()
        self.midlines()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    midlines = midline_creator(village)
    midlines.run()

