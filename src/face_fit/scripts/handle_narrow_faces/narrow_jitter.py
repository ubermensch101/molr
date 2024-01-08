from config import *
from utils import *
import argparse

def narrow_jitter(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Narrow_Jitter(config,pgconn)

class Narrow_Jitter:
    def __init__(self, config: Config, psql_conn: PGConn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        
        self.inp = self.config.setup_details['fbfs']['input_table']
        self.ori = self.config.setup_details['fbfs']['original_faces_table']
        self.nar = self.config.setup_details['fbfs']['narrow_faces_table']
        self.topo = self.village + self.config.setup_details['fbfs']['input_topo_suffix']
        self.nar_mid = self.config.setup_details['fbfs']['narrow_midlines_table']

        self.angle_thresh = self.config.setup_details['fbfs']['corner_nodes_angle_thresh']
        self.survey_no = self.config.setup_details['val']['survey_no_label']
        self.srid = self.config.setup_details['setup']['srid']
        
        self.covered_nodes = config.setup_details['fbfs']['covered_nodes_table']
        self.covered_edges = config.setup_details['fbfs']['covered_edges_table']
        self.covered_faces = config.setup_details['fbfs']['covered_faces_table']
        self.face_node_map = config.setup_details['fbfs']['face_node_map_table']
        
        self.nar_voids = config.setup_details['fbfs']['nar_voids_table']
        self.nar_jitter_buf_thresh = config.setup_details['fbfs']['nar_jitter_buf_thresh']
        self.nar_jitter_cur_face = config.setup_details['fbfs']['nar_jitter_cur_face']
        self.nar_jitter_cur_void = config.setup_details['fbfs']['nar_jitter_cur_void']
        self.fixed_nar_faces_table = config.setup_details['fbfs']['fixed_']
        
        if self.village == "":
            print("ERROR")
            exit()             
    
    def get_voids(self, schema, input_narrow_faces, output_voids_table, farmplots, buf_thresh = 50):
        sql = f"""
            drop table if exists {schema}.{output_voids_table};
            create table {schema}.{output_voids_table} as
            
            with buffered_faces as (
                select
                    face_id as face_id,
                    st_buffer(geom, {buf_thresh}, 'join=mitre') as geom
                from 
                    {schema}.{input_narrow_faces}
            )
            
            select 
                buf.face_id as face_id,
                st_difference(
                    buf.geom,
                    st_union(f.geom)
                ) as geom
            from 
                buffered_faces as buf
            join
                {schema}.{farmplots} as f
                on st_intersects(f.geom, buf.geom)
            group by
                buf.face_id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
    
    def jitter_narrow(self, schema, input_narrow_faces, input_voids_table, cur_face_table, cur_void_table, output_table):
        sql = f"""
            select face_id from {schema}.{input_narrow_faces};
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            nar_faces = curr.fetchall()
        
        for face_id, _ in nar_faces:
            sql = f"""
                drop table if exists {schema}.{cur_face_table};
                create table {schema}.{cur_face_table} as 
                select 
                    geom as geom
                from
                    {schema}.{input_narrow_faces}
                where
                    face_id = {face_id}
                ;
                
                drop table if exists {schema}.{cur_void_table};
                create table {schema}.{cur_void_table} as 
                select 
                    geom as geom
                from
                    {schema}.{input_voids_table}
                where
                    face_id = {face_id}
                ;
            """
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
            
            temp_face_nodes_table = self.config.setup_details['fbfs']['temp_nodes_table']
            temp_updated_face_nodes_table = self.config.setup_details['fbfs']['temp_updated_nodes_table']
            temp_face_table = self.config.setup_details['fbfs']['temp_face_table']
            get_nodes_geom(self.psql_conn, schema, self.topo, temp_face_nodes_table, self.face_node_map, self.covered_nodes, face_id )
            sql = f'''
                select 
                    count(node_id),
                    count(case when shifted_geom = NULL THEN 1 end) 
                from {schema}.{temp_face_nodes_table};
            '''
            with self.psql_conn.connection.cursor() as curr:
                curr.execute(sql)
                counts = curr.fetchall()
            if counts[0][0] == counts[0][1]:
                temp = "temp"
                results = fit_with_area_outside(self.psql_conn, schema, cur_face_table, temp_face_table, cur_void_table, temp, ((-80, 80), (-80, 80)))
                sql = f'''
                    drop table if exists {schema}.{temp_updated_face_nodes_table};
                    create table {schema}.{temp_updated_face_nodes_table} as
            
                    select
                        n.node_id as node_id,
                        case
                            when n.shifted_geom is null 
                            then st_translate( n.original_geom, {results.x[0]}, {results.x[1]} )
                            else n.shifted_geom
                        end as geom
                    from
                    {schema}.{temp_face_nodes_table} as n;
                '''
                with self.psql_conn.connection().cursor() as curr:
                    curr.execute(sql)
            else:
                average_translate_face_nodes(self.psql_conn, self.village, self.topo, face_id, self.face_node_map, self.covered_nodes, temp_updated_face_nodes_table, temp_face_nodes_table )
            get_face(self.psql_conn, self.topo, face_id, schema, temp_face_table, temp_updated_face_nodes_table)
            validity = check_face_valid(self.psql_conn, face_id, schema, temp_face_table, 
                             self.covered_faces, self.covered_edges)
            if not validity:
                continue
            commit_face(self.psql_conn, self.topo, schema, self.covered_nodes, self.covered_edges, self.covered_faces, face_id, temp_face_table, temp_updated_face_nodes_table )    
            
            # NOT COMPLETE
            
    
    def run(self):
        self.get_voids(self.village, self.nar, self.nar_voids, self.farmplots, self.nar_jitter_buf_thresh)
        self.jitter_narrow(self.village, self.nar, self.nar_voids, self.nar_jitter_cur_face, self.nar_jitter_cur_void, )
        
if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    narrow = narrow_jitter(village)
    narrow.run()

