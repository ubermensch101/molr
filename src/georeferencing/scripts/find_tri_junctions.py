from utils import *
from config import *
import json
import argparse

def trijunctions(gcp_label_toggle):
    config = Config()
    pgconn = PGConn(config)
    obj = Trijunctions(config,pgconn)
    if gcp_label_toggle != "":
        obj.option = gcp_label_toggle
    return obj

class Trijunctions:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.schema_name = self.config.setup_details['setup']['village']
        self.survey_jitter = self.config.setup_details['data']['survey_jitter_table']
        self.survey_shifted = self.config.setup_details['georef']['survey_shifted']
        self.survey_shifted_vertices = self.config.setup_details['georef']['survey_shifted_vertices']
        self.survey_jitter_vertices = self.config.setup_details['georef']['survey_jitter_vertices']
        self.option = self.config.setup_details['georef']['gcp_label_toggle']

    def find_tri_junctions(self, input, topo_name, output):
        self.add_village_boundary(input)
        self.create_topo(topo_name, input)
        input_topo_schema = topo_name
        output_table = self.schema_name + "." + output
        sql = f'''
            drop table if exists {output_table};
            create table {output_table} as

            with neigh as (
                select
                    count(edge_id),
                    node_id
                from
                    {input_topo_schema}.edge as p,
                    {input_topo_schema}.node
                where
                    start_node = node_id
                    or end_node = node_id
                group by
                    node_id
            )

            select
                r.node_id as node_id,
                r.geom as geom
            from
                {input_topo_schema}.node as r,
                neigh
            where
                r.node_id = neigh.node_id
                and
                neigh.count > 2
            ;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        self.mapping = self.create_label_for_vertices(output, input )
        self.remove_village_boundary(input)

    def add_village_boundary(self, input):
        table = self.schema_name + "." + input
        sql = f'''
            with bounding_box as
            (SELECT 
                st_expand(
                    st_setSRID(
                        st_extent(geom),
                        32643),
                    10) 
                AS geom FROM {table}
            )
            ,outer_polygon as
            (select
                st_multi(st_difference(
                    (select geom from bounding_box),
                    (select st_makepolygon(st_exteriorring(st_union(geom))) from {table})     
                )) as geom
            )
            insert into {table} (gid,survey_no,geom)
            values(999,(select 'vb'),(select geom from outer_polygon));
        '''
        with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
        
    def create_label_for_vertices(self, input, reference ):
        input_table = self.schema_name + "." + input
        reference_table = self.schema_name + "." + reference
        sql = f'''
            alter table {input_table}
            add column if not exists label varchar(100); 
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

        sql = f'''
            select node_id from {input_table}; 
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            node_ids = curr.fetchall()
        mapping = []
        for res in node_ids:
            node_id = res[0]
            sql = f'''
                select 
                survey_no 
                from 
                    {reference_table}
                where 
                    st_intersects(geom,(select st_buffer(geom,0.1) from {input_table} where node_id = {node_id}))
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
                bordering_survey_no = curr.fetchall()
            survey_no = []
            if len(bordering_survey_no) >= 3:
                for j in bordering_survey_no:
                    if j[0] is not None:
                        if j[0][0]=='G':
                            survey_no.append('g')
                        elif j[0][0]=='S':
                            survey_no.append('rv')
                        elif j[0][0]=='R':
                            survey_no.append('rd')
                        else:
                            survey_no.append(j[0])    
                            
                survey_no.sort()
                mapping.append([node_id, survey_no])
            vertex_label = "-".join([str(i) for i in survey_no])
            sql = f'''
                update {input_table}
                set label = '{vertex_label}'
                where node_id = {node_id};
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)

        return mapping
    
    def remove_village_boundary(self, input):
        table = self.schema_name+"."+input
        sql = f'''
            delete from {table}
            where
            survey_no = 'vb';
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def run(self):
        if self.option=="True":
            used_map = self.survey_shifted
            used_map_vertices = self.survey_shifted_vertices
        elif self.option == "False" :
            used_map = self.survey_jitter
            used_map_vertices = self.survey_jitter_vertices
        topo_name = self.schema_name+"_"+used_map+"_topo"
        self.find_tri_junctions(used_map, topo_name, used_map_vertices)



if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    argument = parser.parse_args()
    gcp_label_toggle = argument.gcp_label_toggle
    trijun = trijunctions(gcp_label_toggle)
    trijun.run()
    
