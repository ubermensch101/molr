from config import *
from utils import *
import argparse

def setup_facefit(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Setup_Facefit(config,pgconn)

class Setup_Facefit:
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

        self.tol = self.config.setup_details['fbfs']['topo_tol']
        self.seg_tol = self.config.setup_details['fbfs']['seg_tol']
        self.seg_length = self.config.setup_details['fbfs']['seg_length']
        self.nar_face_shp_index_thresh = self.config.setup_details['fbfs']['nar_face_shp_index_thresh']
        self.intersection_thresh = self.config.setup_details['fbfs']['survey_no_assignment_intersection_thresh']

        if self.village == "":
            print("ERROR")
            exit()             
            
    def segmentize(self):
        sql_query=f"""
            with edges as (
                select edge_id, start_node, end_node, geom from {self.topo}.edge_data
            ),
            boundary as (
                select
                    (st_dumppoints(st_segmentize(geom, {self.seg_length}))).geom as point
                from
                    edges
            )
            
            select topogeo_addpoint('{self.topo}', point, {self.seg_tol}) from boundary;
        """
        
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            
    def clean_nodes(self):
        sql_query = """
            with two_points as (
                with neigh as (
                    select
                        count(edge_id),
                        node_id
                    from
                        {topo_schema}.edge as p,
                        {topo_schema}.node
                    where
                        start_node = node_id
                        or end_node = node_id
                    group by
                        node_id
                )
                select
                    r.node_id, r.geom
                from
                    {topo_schema}.node as r,
                    neigh
                where
                    r.node_id = neigh.node_id
                    and (neigh.count = 2)
            ),
            narrow_faces as (
                select
                    st_union(st_makevalid(st_getfacegeometry('{topo_schema}', face_id))) as geom
                from
                    {topo_schema}.face
                where
                    face_id>0
                    and
                    st_area(st_makevalid(st_getfacegeometry('{topo_schema}', face_id)))>1
                    and
                    st_perimeter(st_makevalid(st_getfacegeometry('{topo_schema}', face_id))) * 
                        st_perimeter(st_makevalid(st_getfacegeometry('{topo_schema}', face_id))) /
                        st_area(st_makevalid(st_getfacegeometry('{topo_schema}', face_id))) > 55
            )

            select
                p.edge_id as e1,
                q.edge_id as e2
            from
                {topo_schema}.edge as p,
                {topo_schema}.edge as q,
                narrow_faces
            where
                p.edge_id != q.edge_id
                and
                st_intersection(p.geom,q.geom) in (select geom from two_points)
                and
                not st_intersects(st_intersection(p.geom, q.geom), st_buffer(narrow_faces.geom, 5))
                and
                (
                    (degrees(st_angle(p.geom,q.geom)) > 135
                    and degrees(st_angle(p.geom,q.geom)) < 225) or
                    degrees(st_angle(p.geom,q.geom)) < 45
                    or degrees(st_angle(p.geom,q.geom)) > 315
                )
            ;
        """.format(topo_schema = self.topo)

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
            all_pairs=curs.fetchall()
        
        visited=[]
        for pairs in all_pairs:
            if pairs[0] in visited or pairs[1] in visited:
                continue

            sql_query="""select st_newedgeheal('{topo_schema}', {pair1}, {pair2})""".format(
                topo_schema=self.topo, pair1=pairs[0], pair2=pairs[1]
            )

            with self.psql_conn.connection().cursor() as curs:
                curs.execute(sql_query)

            visited.append(pairs[0])
            visited.append(pairs[1])
    
    def make_faces(self):
        sql_query=f"""
            select polygonize('{self.topo}');

            drop table if exists {self.village}.{self.ori};
            create table {self.village}.{self.ori} as
            
            select
                face_id,
                st_makevalid(st_getfacegeometry('{self.topo}', face_id)) as geom
            from
                {self.topo}.face
            where
                face_id>0
                and
                st_perimeter(st_makevalid(st_getfacegeometry('{self.topo}', face_id))) * 
                    st_perimeter(st_makevalid(st_getfacegeometry('{self.topo}', face_id))) /
                    st_area(st_makevalid(st_getfacegeometry('{self.topo}', face_id))) < {self.nar_face_shp_index_thresh}
            ;


            drop table if exists {self.village}.{self.nar};
            create table {self.village}.{self.nar} as
            
            select
                face_id,
                st_makevalid(st_getfacegeometry('{self.topo}', face_id)) as geom
            from
                {self.topo}.face
            where
                face_id>0
                and
                st_area(st_makevalid(st_getfacegeometry('{self.topo}', face_id)))>1
                and
                st_perimeter(st_makevalid(st_getfacegeometry('{self.topo}', face_id))) * 
                    st_perimeter(st_makevalid(st_getfacegeometry('{self.topo}', face_id))) /
                    st_area(st_makevalid(st_getfacegeometry('{self.topo}', face_id))) > {self.nar_face_shp_index_thresh}
            ;


            drop table if exists {self.village}.{self.nar_mid};
            create table {self.village}.{self.nar_mid} as

            select
                face_id,
                st_approximatemedialaxis(geom) as geom
            from
                {self.village}.{self.nar}
            where
                st_area(geom)>1
            ;
        """
        
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
        
        sql_query = f"""
            update {self.village}.{self.ori}
            set geom = st_multi(geom);
            
            alter table {self.village}.{self.ori}
            add column if not exists survey_no varchar(100) default '',
            add column if not exists akarbandh_area float,
            add column if not exists valid bool,
            add column if not exists varp float,
            add column if not exists shape_index float,
            add column if not exists farm_intersection float,
            add column if not exists farm_rating float;
            
            update {self.village}.{self.ori} o
            set survey_no = a.survey_no,
                akarbandh_area = a.akarbandh_area,
                valid = a.valid,
                varp = a.varp,
                shape_index = a.shape_index,
                farm_intersection = a.farm_intersection,
                farm_rating = a.farm_rating
            from {self.village}.{self.inp} as a
            where st_area(st_intersection(o.geom,a.geom))/st_area(o.geom) > {self.intersection_thresh};
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
        add_gist_index(self.psql_conn, self.village, self.ori, 'geom')
        add_gist_index(self.psql_conn, self.village, self.nar, 'geom')

    def run(self):
        create_topo(self.psql_conn, self.village, self.topo, self.inp, self.tol)
        self.segmentize()
        for _ in range(7):
            self.clean_nodes()
        self.make_faces()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    midlines = setup_facefit(village)
    midlines.run()

