from utils import *

class Node_Selector:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        
        self.inp = self.config.setup_details['fbfs']['input_table']
        self.ori = self.config.setup_details['fbfs']['original_faces_table']
        self.nar = self.config.setup_details['fbfs']['narrow_faces_table']
        self.topo = self.village + self.config.setup_details['fbfs']['input_topo_suffix']

        self.angle_thresh = self.config.setup_details['fbfs']['corner_nodes_angle_thresh']
        self.survey_no = self.config.setup_details['val']['survey_no_label']
        self.srid = self.config.setup_details['setup']['srid']
        
        self.covered_nodes = config.setup_details['fbfs']['covered_nodes_table']
        self.covered_edges = config.setup_details['fbfs']['covered_edges_table']
        self.covered_faces = config.setup_details['fbfs']['covered_faces_table']
        self.face_node_map = config.setup_details['fbfs']['face_node_map_table']
        
        self.temp_nodes_geom_table = config.setup_details['fbfs']['temp_nodes_geom_table']
        self.temp_translate_nodes = config.setup_details['fbfs']['temp_translate_nodes']
        self.temp_possible_snaps_table = config.setup_details['fbfs']['temp_possible_snaps']
        self.filtered_temp_possible_snaps_table = config.setup_details['fbfs']['filtered_temp_possible_snaps']
        
        self.snap_buffer_thresh = config.setup_details['fbfs']['snap_buffer_thresh']
        self.point_in_void_distance_thresh = config.setup_details['fbfs']['point_in_void_distance_thresh']
        self.possible_snaps_thresh_1 = int(config.setup_details['fbfs']['possible_snaps_thresh_1'])
        self.possible_snaps_thresh_2 = int(config.setup_details['fbfs']['possible_snaps_thresh_2'])
    

        self.vfn_table = config.setup_details['fp']['valid_farm_nodes_table']
        
    def add_shifted_nodes(self, output_table):
        sql = f"""
            insert into {output_table}
            select
                node_id as node_id,
                1 as id,
                shifted_geom as geom
            from
                {self.village}.{self.temp_nodes_geom_table}
            where 
                shifted_geom is not null
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    def get_unshifted_node_ids(self):
        sql = f"""
            with unshifted_nodes as (
                select
                    node_id,
                    original_geom as geom
                from
                    {self.village}.{self.temp_nodes_geom_table}
                where
                    shifted_geom is null
            ),
            degrees as (
                select
                    n.node_id as node_id,
                    n.geom as geom,
                    count(e.edge_id) as degree
                from
                    unshifted_nodes as n,
                    {self.topo}.edge_data as e
                where
                    (e.start_node = n.node_id or e.end_node = n.node_id)
                group by 
                    n.node_id, n.geom
            ),
            fp as (
                select
                    st_collect(geom) as geom
                from
                    {self.village}.{self.farmplots}
            )
            select
                d.node_id,
                not st_intersects(
                    st_buffer(d.geom,{self.point_in_void_distance_thresh},'join=mitre'), 
                    f.geom
                )
            from
                degrees as d,
                fp as f
            
            order by
                degree desc
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            node_ids = curr.fetchall()
        
        return node_ids
    
    def add_translate_option(self, output_table, node_id):
        sql = f"""
            with covered_area as (
                select
                    st_collect(st_collect(e.geom), st_collect(f.geom)) as geom
                from
                    {self.village}.{self.covered_edges} as e,
                    {self.village}.{self.covered_faces} as f
            )
            insert into {output_table} (node_id, geom)
            select
                n.node_id as node_id,
                n.geom as geom
            from
                {self.village}.{self.temp_translate_nodes} as n,
                covered_area as c
            where
                n.node_id = {node_id}
                and
                not coalesce(st_intersects(c.geom, n.geom),false)
            returning
                node_id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            output = curr.fetchall()
            
        if output is None or len(output)==0:
            return False
        else:
            return True
        
    def add_farm_node_options(self, output_table, node_id, max_snaps):
        print(node_id,max_snaps)
        sql = f"""
            with cur_node as (
                select
                    node_id,
                    geom
                from
                    {self.village}.{self.temp_translate_nodes}
                where
                    node_id = {node_id}
            ),
            distanced_farm_nodes as (
                select
                    n.node_id as node_id,
                    fn.geom as geom,
                    st_distance(n.geom, fn.geom) as distance
                from
                    cur_node as n
                inner join
                    {self.village}.{self.vfn_table} as fn
                    on st_dwithin(n.geom,fn.geom,{self.snap_buffer_thresh})
            ),
            covered_area as (
                select
                    st_collect(st_collect(e.geom), st_collect(f.geom)) as geom
                from
                    {self.village}.{self.covered_edges} as e,
                    {self.village}.{self.covered_faces} as f
            )
            insert into {output_table} (node_id, geom)
            select
                n.node_id as node_id,
                n.geom as geom
            from
                distanced_farm_nodes as n,
                covered_area as c
            where
                not coalesce(st_intersects(c.geom, n.geom),false)
            order by
                n.distance
            limit
                {max_snaps}
            returning
                node_id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            output = curr.fetchall()
            print(output)
        if output is None or len(output)==0:
            return 0
        else:
            return len(output)
            
        # add the possible snaps for the given node_id
    
    def filter_options(self, input_table, max_snaps, output_table, node_id):
        sql = f"""
            with cur_node as (
                select
                    node_id,
                    geom
                from
                    {self.village}.{self.temp_translate_nodes}
                where
                    node_id = {node_id}
            ),
            distanced_nodes as (
                select
                    inp.node_id as node_id,
                    inp.id as id,
                    inp.geom as geom,
                    st_distance(n.geom, inp.geom) as distance
                from
                    cur_node as n,
                    {input_table} as inp
            )
            insert into {output_table} (node_id, id, geom)
            select
                node_id,
                id,
                geom
            from
                distanced_nodes
            order by
                distance
            limit
                {max_snaps}
            returning
                id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            output = curr.fetchall()
            
        if output is None or len(output)==0:
            return 0
        else:
            return len(output)
            
    def add_possible_snaps(self, input_table, output_table):
        sql = f"""
            insert into {output_table} (node_id, id, geom)
            select
                node_id,
                id,
                geom
            from
                {input_table}
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
    
    def create_possible_snaps_table(self, table, srid):
        sql = f"""
            drop table if exists {table};
            create table {table} (
                node_id integer,
                id integer,
                geom geometry(Point, {srid}),
                primary key (node_id,id)  
            );
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
    
    def get_possible_snaps(self, face_id, possible_snaps_table):
        
        get_nodes_geom(self.psql_conn, self.village, self.topo, self.temp_nodes_geom_table,
                                    self.face_node_map, self.covered_nodes, face_id)        
        average_translate_face_nodes(self.psql_conn, self.village, self.topo, face_id, self.face_node_map,
                                     self.covered_nodes, self.temp_translate_nodes, self.temp_nodes_geom_table)
        
        self.create_possible_snaps_table(possible_snaps_table, self.srid)
        
        self.add_shifted_nodes(possible_snaps_table)
        
        node_ids = self.get_unshifted_node_ids()
        
        for node_id, node_in_void in node_ids:
            
            sql = f"""
                drop table if exists {self.village}.{self.temp_possible_snaps_table};
                create table {self.village}.{self.temp_possible_snaps_table} (
                    node_id integer,
                    id serial,
                    geom geometry(Point, {self.srid})
                );
                drop table if exists {self.village}.{self.filtered_temp_possible_snaps_table};
                create table {self.village}.{self.filtered_temp_possible_snaps_table} (
                    node_id integer,
                    id integer default 1,
                    geom geometry(Point, {self.srid})
                );
            """
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
                
            count = 0
            if node_in_void:
                if self.add_translate_option(self.village+'.'+self.temp_possible_snaps_table, node_id):
                    count += 1
            
            print("adding farmnode option")
            num_added = self.add_farm_node_options(self.village+'.'+self.temp_possible_snaps_table,node_id, 
                                       self.possible_snaps_thresh_1-count)
            count += num_added
            
            number_selected = self.filter_options(self.village+'.'+self.temp_possible_snaps_table, self.possible_snaps_thresh_2, 
                                                  self.village+'.'+self.filtered_temp_possible_snaps_table, node_id)
            
            print(num_added, number_selected)
            if number_selected==0:
                if not self.add_translate_option(self.village+'.'+self.filtered_temp_possible_snaps_table, node_id):
                    return
            print(f"Snaps for nodes stored in {self.filtered_temp_possible_snaps_table}")
            a = input("Press Enter to continue : ")
            self.add_possible_snaps(self.village+'.'+self.filtered_temp_possible_snaps_table, possible_snaps_table)
            
        sql = f"""
            select 
                node_id,
                array_agg(id)
            from
                {possible_snaps_table}
            group by
                node_id
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            snap_ids = curr.fetchall()
            
        print(f"Found Possible snaps for face_id {face_id} as", snap_ids)
        a = input("Press Enter to continue : ")
        return snap_ids
                
                
                