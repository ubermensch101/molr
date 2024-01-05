from config import *
from utils import *
from helper_classes import *
import argparse
from itertools import product

def snap_fit(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Snap_Fit(config,pgconn)

class Snap_Fit:
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
        
        self.visited_faces = config.setup_details['fbfs']['visited_faces_table']
        
        self.shifted_faces = config.setup_details['fbfs']['shifted_faces_table']
        
        if self.village == "":
            print("ERROR")
            exit()
            
    def setup_snap_fit(self):
        # add all field which are to be tracked
        add_varp(self.psql_conn, self.village, self.ori, 'varp')
        sql = f"""
            drop table if exists {self.village}.{self.visited_faces};
            create table {self.village}.{self.visited_faces} (face_id integer primary key);
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        self.update_visited_faces()
    
    def update_visited_faces(self, face_id=None):
        sql = f"""
            insert into {self.village}.{self.visited_faces}
            select
                face_id as face_id
            from 
                {self.village}.{self.covered_faces}
            on conflict do nothing
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        
        if face_id != None:
            sql = f"""
                insert into {self.village}.{self.visited_faces} (face_id)
                values ({face_id})
                on conflict do nothing
                ;
            """
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
    
    def evaluate_face(self, schema, table, farmplots, reference):
        add_varp(self.psql_conn, schema, table, 'varp')
        add_farm_intersection(self.psql_conn, schema, table, farmplots, 'farm_intersection')
        add_farm_rating(self.psql_conn, schema, table, farmplots, 'farm_rating')
        add_shape_index(self.psql_conn, schema, table, 'shape_index')
        
        sql = f"""
            select
                t.farm_rating as farm_rating,
                t.varp - r.varp as varp_dif,
                abs((st_area(t.geom)/st_area(r.geom))-1)*100 as area_dif,
                t.shape_index as shape_index
            from
                {schema}.{table} as t
            join
                {schema}.{reference} as r
                on t.face_id = r.face_id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            vals = curr.fetchone()
        
        return vals    
    
    # threshhold and metrics are assumed to be in order : [farm_rating, varp_diff, area_diff, shape_index]
    def compare_metrics(self, metrics, best_metrics, thresholds):
        return metrics[0]>best_metrics[0] and metrics[0]>thresholds[0] and \
            metrics[1]<thresholds[1] and metrics[2]<thresholds[2] and metrics[3]<thresholds[3]
            
    def cover_face(self, face_id, thresholds):
        possible_node_snaps_table = self.config.setup_details['fbfs']['possible_node_snaps']
        
        ns = Node_Selector(self.config, self.psql_conn)
        node_ids, possible_snap_ids = ns.get_possible_snaps(face_id, self.village, 
                                                            possible_node_snaps_table)
        
        best_metrics = [0, 0, 0, 0]
        best_face_nodes_table = self.config.setup_details['fbfs']['best_nodes_table'] 
        best_face_table = self.config.setup_details['fbfs']['best_face_table']
        temp_face_nodes_table = self.config.setup_details['fbfs']['temp_nodes_table']
        temp_face_table = self.config.setup_details['fbfs']['temp_face_table']
        
        create_nodes_table(self.psql_conn, self.village, best_face_nodes_table, self.srid)
        
        for i in product(*possible_snap_ids):
            
            create_nodes_table(self.psql_conn, self.village, temp_face_nodes_table, self.srid)
            
            for j, node_id in enumerate(node_ids):
                # add in temp_shifted_nodes the geom corresponding to id i[j] for node_id
                # make the face
                sql = f"""
                    with point as (
                        select
                            node_id,
                            geom
                        from
                            {self.village}.{possible_node_snaps_table}
                        where
                            node_id = {node_id}
                            and
                            id = {i[j]}
                    )
                    insert into {self.village}.{temp_face_nodes_table} (node_id, geom)
                    select
                        node_id,
                        geom
                    from
                        point
                    limit
                        1
                    on conflict do nothing;
                """
                with self.psql_conn.connection().cursor() as curr:
                    curr.execute(sql)
            
            get_face(self.psql_conn, self.topo, face_id, self.village, temp_face_table, 
                     temp_face_nodes_table, 'node_id', 'geom')
            validity = check_face_valid(self.psql_conn, face_id, self.village, temp_face_table, 
                             self.covered_faces, self.covered_edges)
            if not validity:
                continue
            
            metrics = self.evaluate_face(face_id, self.village, temp_face_table, self.farmplots, self.inp)
            
            if self.compare_metrics(metrics,best_metrics,thresholds):
                copy_table(self.psql_conn, self.village+'.'+temp_face_nodes_table, 
                           self.village+'.'+best_face_nodes_table)
                copy_table(self.psql_conn, self.village+'.'+temp_face_table, 
                           self.village+'.'+best_face_table)
            
        if best_metrics == [0,0,0,0]:
            print(f"skipping face {face_id}")
        else:
            commit_face(self.psql_conn, self.topo, self.village, self.covered_nodes, self.covered_edges,
                        self.covered_faces, face_id, best_face_table, best_face_nodes_table, 'node_id', 'geom')
        
        self.update_visited_faces(face_id)
    
    def finalize_outputs(self):
        sql = f"""
            drop table if exists {self.village}.{self.shifted_faces};
            create table {self.village}.{self.shifted_faces} as 
            select 
                ori.face_id,
                covered.geom,
                ori.survey_no,
                ori.akarbandh_area,
                ori.valid
            from
                {self.village}.{self.covered_faces} as covered
            join
                {self.village}.{self.ori} as ori
                on covered.face_id = ori.face_id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        
    def run(self):
        self.setup_snap_fit()
        
        thresholds = [0.96, 0.8, 10, 500]
        
        sched = Face_Schedular(self.config, self.psql_conn)
        while True:
            next_face_id = sched.next_face()
            if next_face_id == None:
                break
            self.cover_face(next_face_id, thresholds)
        
        
        thresholds = [0, 1.5, 1000, 500]
        
        sched = Face_Schedular(self.config, self.psql_conn)
        while True:
            next_face_id = sched.next_face()
            if next_face_id == None:
                break
            self.cover_face(next_face_id, thresholds)
        
        self.finalize_outputs()
    
if __name__ == "__main__":    
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    sf = snap_fit(village)
    sf.run()

