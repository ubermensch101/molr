from queue import PriorityQueue

class Face_Scheduler:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        
        self.ori = self.config.setup_details['fbfs']['original_faces_table']

        self.covered_nodes = config.setup_details['fbfs']['covered_nodes_table']
        self.covered_edges = config.setup_details['fbfs']['covered_edges_table']
        self.covered_faces = config.setup_details['fbfs']['covered_faces_table']
        self.face_node_map = config.setup_details['fbfs']['face_node_map_table']
        
        self.visited_faces = config.setup_details['fbfs']['visited_faces_table']
        
        # self.priority_queue = PriorityQueue()
        # self.setup_priority_queue()
        
    # def setup_priority_queue(self):
    #     sql = f"""
    #         with faces as (
    #             select 
    #                 face_id
    #             from
    #                 {self.village}.{self.ori}
    #         ),
    #         covered_faces as (
    #             select 
    #                 face_id
    #             from
    #                 {self.village}.{self.covered_faces}
    #         ),
    #         uncovered_faces as (
    #             select
    #                 f.face_id as face_id
    #             from 
    #                 faces as f
    #             left join
    #                 covered_faces as cf
    #                 on f.face_id = cf.face_id
    #             where
    #                 cf.face_id is null
    #         ),
    #         remaining_nodes as (
    #             select 
    #                 fnmap.face_id as face_id,
    #                 fnmap.node_id as node_id
    #             from
    #                 {self.village}.{self.face_node_map} as fnmap
    #             left join
    #                 {self.village}.{self.covered_nodes} as cn
    #                 on fnmap.node_id = cn.node_id
    #             where
    #                 cn.node_id is null
    #         )
    #         select 
    #             count(rn.node_id) as count,
    #             uf.face_id
    #         from
    #             uncovered_faces as uf
    #         join
    #             remaining_nodes as rn
    #             on uf.face_id = rn.face_id
    #         group by
    #             uf.face_id
    #         ;
    #     """
    #     with self.psql_conn.connection().cursor() as curr:
    #         curr.execute(sql)
    #         priority_list = curr.fetchall()
            
    #     _ = list(map(lambda x:self.priority_queue.put(x), priority_list))
        
    def next_face(self):
        sql = f"""
            with faces as (
                select 
                    face_id
                from
                    {self.village}.{self.ori}
            ),
            visited_faces as (
                select 
                    face_id
                from
                    {self.village}.{self.visited_faces}
            ),
            uncovered_faces as (
                select
                    f.face_id as face_id
                from 
                    faces as f
                left join
                    visited_faces as cf
                    on f.face_id = cf.face_id
                where
                    cf.face_id is null
            ),
            remaining_nodes as (
                select 
                    fnmap.face_id as face_id,
                    fnmap.node_id as node_id
                from
                    {self.village}.{self.face_node_map} as fnmap
                left join
                    {self.village}.{self.covered_nodes} as cn
                    on fnmap.node_id = cn.node_id
                where
                    cn.node_id is null
            )
            select 
                count(rn.node_id) as count,
                uf.face_id
            from
                uncovered_faces as uf
            join
                remaining_nodes as rn
                on uf.face_id = rn.face_id
            group by
                uf.face_id
            ;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            priority_list = curr.fetchall()
        if len(priority_list) == 0:
            return None
        else:
            return sorted(priority_list)[0][1]

