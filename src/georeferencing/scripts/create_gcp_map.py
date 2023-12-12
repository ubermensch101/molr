import re


class GCP_map:
    def __init__(self, config , psql_conn ):
        self.config = config
        self.psql_conn = psql_conn
        self.schema_name = self.config.setup_details['setup']['village']
        self.gcp = self.config.setup_details['data']['gcp_table']
        self.gcp_map = self.config.setup_details['georef']['gcp_map']
        self.delimiter = self.config.setup_details['georef']['delimiter']
        self.survey_shifted_vertices = self.config.setup_details['georef']['survey_shifted_vertices']
        self.survey_jitter_vertices = self.config.setup_details['georef']['survey_jitter_vertices']
        self.gcp_label_toggle = self.config.setup_details['georef']['gcp_label_toggle']
        self.gcp_label_column = self.config.setup_details['georef']['gcp_label_column']

    def create_gcp_map(self, input, gcp, output):
        input_table = self.schema_name + "." + input
        gcp_table = self.schema_name + "." + gcp
        output_table = self.schema_name + "." + output
        sql = f'''
            drop table if exists {output_table};
            create table {output_table} as
            
            with gcp as (
                select
                    st_collect(st_transform(geom,32643)) as geom
                from
                    {gcp_table}
            ),
            point as (
                select
                    node_id,
                    st_transform(geom,32643) as geom
                from
                    {input_table}
            )

            select
                point.node_id as node_id,
                point.geom as geom,
                st_closestpoint(gcp.geom, point.geom) as gcp_geom
            from
                point, gcp
            where
                st_distance(gcp.geom,point.geom) < 30;
                
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

        sql = f'''
            alter table {gcp_table}
            add column if not exists {self.gcp_label_column} varchar(100); 
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

        sql = f'''
            select node_id , gid from {output_table}; 
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            read = curr.fetchall()
        for res in read:
            node_id = res[0]
            gid = res[1]
            sql = f'''
                select 
                    label 
                from 
                    {input_table}
                where 
                    node_id = {node_id};
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
                label = curr.fetchall()
            label = label[0][0]
            sql = f'''
                update {gcp_table}
                set {self.gcp_label_column} = '{label}'
                where gid = {gid};
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)


    def create_gcp_map_using_labels(self, input_vertices, gcp, output):
        vertices_labels = self.get_map_labels(input_vertices, "label" )
        gcp_labels = self.get_gcp_labels( gcp, self.gcp_label_column)
        map = []

        for s_vertex in vertices_labels:
            for g_vertex in gcp_labels:
                if s_vertex[1] == g_vertex[1]:
                    map.append([s_vertex[0], g_vertex[0], s_vertex[1]])

        output_table= self.schema_name + "." + output
        gcp_table = self.schema_name + "." + gcp
        input_vertices_table = self.schema_name + "." + input_vertices
        sql = f'''
            drop table if exists {output_table};
            create table {output_table}
            (gid int, node_id int, geom geometry(Point,32643), gcp_geom geometry(Point,32643));
            ALTER TABLE {gcp_table}  
            ALTER COLUMN geom TYPE geometry(POINT, 32643) 
            USING ST_Force2D(geom);
            ALTER TABLE {input_vertices_table}  
            ALTER COLUMN geom TYPE geometry(POINT, 32643) 
            USING ST_Force2D(geom);
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

        for mapping in map:
            tri_junction_gid = mapping[0]
            gcp_gid = mapping[1]
            sql = f'''

                with gcp_table as 
                    (select (st_dump((geom))).geom, gid from {gcp_table})
                insert into {output_table}
                select 
                    p.gid as gid, q.node_id as node_id, q.geom as geom, p.geom as gcp_geom
                from
                    gcp_table as p, {input_vertices_table} as q
                where
                    p.gid = {gcp_gid}
                and
                    q.node_id = {tri_junction_gid}
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)

    def get_map_labels(self, input, input_label_column):
        input_table = self.schema_name + "." + input
        sql = f'''
            select gid,{input_label_column} 
            from
                {input_table}
            '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            a = curr.fetchall()
        mapping = []
        for res in a:
            survey_no = re.split(self.delimiter,res[1])
            survey_no.sort()
            if len(survey_no) >= 3:
                mapping.append([res[0], survey_no])
        return mapping

    def run(self):
        if self.gcp_label_toggle == "True":
            used_map_vertices = self.survey_shifted_vertices
            self.create_gcp_map_using_labels(used_map_vertices, self.gcp, self.gcp_map)
        elif self.gcp_label_toggle == "False":
            used_map_vertices = self.survey_jitter_vertices
            self.create_gcp_map(used_map_vertices, self.gcp, self.gcp_map)
