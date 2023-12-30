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
        self.gcp = self.config.setup_details['data']['gcp_table']
        
        self.inp = self.config.setup_details['fbfs']['input_table']
        self.ori = self.config.setup_details['fbfs']['original_faces_table']
        self.nar = self.config.setup_details['fbfs']['narrow_faces_table']
        self.topo = self.village + self.config.setup_details['fbfs']['input_topo_suffix']
        self.nar_mid = self.config.setup_details['fbfs']['narrow_midlines_table']
        self.nodes = self.config.setup_details['fbfs']['corner_nodes']
        self.nodes_label = self.config.setup_details['fbfs']['corner_nodes_label_column']
        self.nodes_buf_thresh = self.config.setup_details['fbfs']['corner_nodes_label_buf_thresh']
        self.gcp_label = self.config.setup_details['val']['gcp_label']
        self.vb_label = self.config.setup_details['val']['vb_gcp_label']
        self.gcp_labeling_convention = self.config.setup_details['val']['gcp_label_convention']
        self.gcp_map = self.config.setup_details['fbfs']['gcp_map_table']
        self.label_delim = self.config.setup_details['val']['label_delimiter']
        self.shifted_nodes = self.config.setup_details['fbfs']['shifted_nodes_table']
        self.cadastral_topo = self.config.setup_details['fbfs']['cadastral_topo']
        self.nar_nodes = self.config.setup_details['fbfs']['narrow_face_nodes']

        self.tol = self.config.setup_details['fbfs']['topo_tol']
        self.seg_tol = self.config.setup_details['fbfs']['seg_tol']
        self.seg_length = self.config.setup_details['fbfs']['seg_length']
        self.nar_face_shp_index_thresh = self.config.setup_details['fbfs']['nar_face_shp_index_thresh']
        self.intersection_thresh = self.config.setup_details['fbfs']['survey_no_assignment_intersection_thresh']
        
        self.angle_thresh = self.config.setup_details['fbfs']['corner_nodes_angle_thresh']
        self.survey_no = self.config.setup_details['val']['survey_no_label']
        self.srid = self.config.setup_details['setup']['srid']
        
        if self.village == "":
            print("ERROR")
            exit()             
    
    def create_narrow_nodes_table(self):
        cadastral_topo_schema = f"{self.village}{self.cadastral_topo}"
        sql_query = f"""
            drop table if exists {self.village}.{self.nar_nodes};
            create table {self.village}.{self.nar_nodes} as

            with narrow_faces as (
                select face_id, geom from {self.village}.{self.nar}
            ),
            mapped_gcps as (
                select st_collect(geom) as geom from {self.village}.{self.gcp_map}
            )

            select
                nodes.node_id as node_id,
                nodes.geom as geom,
                narrow_faces.face_id as narrow_face_id
            from
                {cadastral_topo_schema}.node nodes,
                narrow_faces narrow_faces
            where
                st_intersects(st_buffer(narrow_faces.geom, 0.5), nodes.geom)
            ;
        """

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()

    def buffer_around_narrow_faces(self):
        sql_query = f"""
            drop table if exists {self.village}.roads_buf;
            create table {self.village}.roads_buf as

            select
                face_id,
                geom as original_geom,
                st_buffer(geom, 50) as buffer_geom
            from
                {self.village}.{self.nar}
            ;
        """

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()

    def create_void_for_narrow_faces(self):
        sql_query = f"""
            drop table if exists {self.village}.road_void_space;
            create table {self.village}.road_void_space as

            with farm_in_road_buf as (
                select
                    roads.face_id as face_id,
                    roads.original_geom as original_geom,
                    roads.buffer_geom as buffer_geom,
                    st_union(farm_plots.geom) as farm_geom
                from
                    {self.village}.farmplots_dedup farm_plots,
                    {self.village}.roads_buf roads
                where
                    st_intersects(roads.buffer_geom, farm_plots.geom)
                group by
                    roads.face_id,
                    roads.original_geom,
                    roads.buffer_geom
            )

            select
                face_id,
                original_geom,
                st_difference(buffer_geom, farm_geom) as void_geom
            from
                farm_in_road_buf
            ;
        """

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()

    def create_buffered_narrow_faces_table(self):
        sql_query = f"""
            drop table if exists {self.village}.all_roads;
            create table {self.village}.all_roads (
                face_id int,
                geom geometry(Geometry, 32643)
            );

            select face_id from {self.village}.roads_buf;
        """

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
            road_face_id_fetch = curs.fetchall()
        
        road_face_id = [road[0] for road in road_face_id_fetch]
        for face_id in road_face_id:
            self.initialise_fit(face_id)
            self.fit_face(face_id)

    def initialise_fit(self, face_id):
        # select current road with given
        # face id from buffered narrow faces
        # and select current void road with given
        # same face_id and form a table for it
    
        sql_query = """
            drop table if exists {schema}.cur_road;
            create table {schema}.cur_road as

            select
                face_id,
                original_geom as geom
            from
                {schema}.roads_buf
            where
                face_id = {face_id}
            ;


            drop table if exists {schema}.cur_void;
            create table {schema}.cur_void as

            select
                face_id,
                void_geom as geom
            from
                {schema}.road_void_space
            where
                face_id = {face_id}
            ;
        """.format(schema=self.village, face_id=face_id)

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()

    def fit_face(self, face_id):
        bnds = ((-0.001, 0.001), (1, 1.001), (1, 1.001), (-80, 80), (-80, 80))
        # jitter_fit(self.psql_conn, self.village, f"{self.village}.cur_road")
        result = minimize(self.area_outside, [0, 0, 0, 0, 0], args=(self.psql_conn.connection(),
            f"{self.village}.cur_road", f"{self.village}.cur_void",
            f"{self.village}.fixed_road"), bounds=bnds)
        
        trans_params = result.x
        
        
        sql_query = """
            insert into {schema}.all_roads
            select
                {face_id} as face_id,
                geom
            from
                {schema}.fixed_road
            ;
        """.format(schema=self.village, face_id=face_id)
        # the fixed road is added into the all roads table (why???)
        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()
        self.wind_up_narrow_face_jitter(face_id, trans_params)

    def wind_up_narrow_face_jitter(self, face_id, trans_params):
        self.translate_nodes(self.psql_conn, face_id, trans_params)

        delta_x = trans_params[3]
        delta_y = trans_params[4]

        sql_query = f'''
            drop table if exists {self.village}.mapped_polygon;
            create table {self.village}.mapped_polygon as

            select
                narrow_faces.face_id as face_id,
                st_translate(geom, {delta_x},{delta_y}) as geom
            from
                {self.village}.narrow_faces narrow_faces
            where
                face_id = {face_id}
            ;
        '''
        # here the narrow face is added to the table
        # named  mapped polygon which includes all
        # the polygons which are mapped (or fixed)
        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()

        sql_query="""
            select (st_getfaceedges('{cadastral_topo_schema}', {face_id})).*;
        """.format(cadastral_topo_schema=self.cadastral_topo, face_id=face_id)
        # get edges of the selected road or current road
        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
            face_edges=curs.fetchall()
            edge_ids=[abs(edge[1]) for edge in face_edges]
        
        sql_query="""
            drop table if exists {schema}.temp_polygon_edges;
            create table {schema}.temp_polygon_edges (
                edge_id integer,
                geom geometry(LineString, 32643)
            );
        """.format(schema=self.village)

        with self.psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        self.psql_conn.connection().commit()
        
        for edge_id in edge_ids:
            self.make_polygon_edges(self.psql_conn, "shifted_nodes", edge_id)
        
        update_covered_area(self.psql_conn, self.village)
        update_covered_edges(self.psql_conn, self.village, self.cadastral_topo)

    def area_outside(self, parameters, psql_conn, input_table, reference_table, temporary_table):
        excess_area(parameters, psql_conn, self.village, input_table, reference_table, temporary_table)
        psql_conn.connection().commit()

        sql_query = f"""
            select
                st_area(st_difference(temp.geom, void.geom)) as area_diff
            from
                {temporary_table} temp,
                {reference_table} void
            ;
        """

        with psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
            area_diff = curs.fetchone()[0]

        distance = np.sqrt(parameters[3]**2 + parameters[4]**2)
        loss = area_diff + distance**2
        print(area_diff, loss)
        
        return loss
    
    def translate_nodes(self, psql_conn, narrow_face_id, trans_params):
        delta_x = trans_params[3]
        delta_y = trans_params[4]

        sql_query = """
            select
                node_id
            from
                {schema}.narrow_face_nodes
            where
                narrow_face_id = '{narrow_face_id}'
            ;
        """.format(schema=self.village, narrow_face_id=narrow_face_id)

        with psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
            nodes_fetch = curs.fetchall()
        
        narrow_nodes = [node[0] for node in nodes_fetch]

        for node_id in narrow_nodes:
            if self.check_node_shifted(psql_conn.connection(), node_id):
                continue

            sql_query = """
                insert into {schema}.shifted_nodes

                select
                    node_id,
                    st_translate(geom, {delta_x}, {delta_y}) as geom
                from
                    {cadastral_topo_schema}.node
                where
                    node_id = '{node_id}'
                ;
            """.format(schema=self.village, cadastral_topo_schema=self.cadastral_topo,
                node_id=node_id, delta_x=delta_x, delta_y=delta_y)
            
            with psql_conn.connection().cursor() as curs:
                curs.execute(sql_query)
            psql_conn.connection().commit()

    def check_node_shifted(self, psql_conn, node_id):
        sql_query="""
            select * from {schema}.shifted_nodes where node_id={node_id};
        """.format(schema=self.village, node_id=node_id)

        with psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
            shifted_node=curs.fetchone()
        
        if shifted_node is not None:
            return True
        else:
            return False

    def make_polygon_edges(self, psql_conn, shifted_nodes_table, edge_id):
        sql_query="""
            with edge as (
                select
                    edge_id,
                    start_node,
                    end_node
                from
                    {cadastral_topo_schema}.edge
                where
                    edge_id={edge_id}
            ),
            nodes as (
                select
                    node_id,
                    geom
                from
                    {schema}.{shifted_nodes_table}
            )

            insert into {schema}.temp_polygon_edges
            select
                edge_id,
                st_makeline(nodes1.geom, nodes2.geom) as geom
            from
                edge,
                nodes nodes1,
                nodes nodes2
            where
                edge.start_node=nodes1.node_id
                and edge.end_node=nodes2.node_id
            ;
        """.format(cadastral_topo_schema=self.cadastral_topo,
            schema=self.village, edge_id=edge_id, shifted_nodes_table=shifted_nodes_table)
        
        with psql_conn.connection().cursor() as curs:
            curs.execute(sql_query)
        psql_conn.connection().commit()

    def run(self):
        self.create_narrow_nodes_table()
        self.buffer_around_narrow_faces()
        self.create_void_for_narrow_faces()
        self.create_buffered_narrow_faces_table()
        # now the create narrow faces will 
        
if __name__ == "__main__":    
    from face_fit_utils import *
    
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    narrow = narrow_jitter(village)
    narrow.run()

else:
    from .face_fit_utils import *

