from utils import *
from config import *
from scripts import *
import json

def georeferencer():
    config = Config()
    
    pgconn = PGConn(config)
    
    return Georeferencer(config,pgconn)

class Georeferencer:
    def __init__(self,config,psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.survey_jitter =  self.config.setup_details['data']['survey_jitter_table']
        self.schema_name = self.config.setup_details['setup']['village']
        self.farmplots = self.config.setup_details['data']['farmplots_table']
        self.survey_georeferenced = self.config.setup_details['data']['survey_georeferenced_table']
        self.final_gcps_used = []
        self.final_output = [""]


    def jitter(self):
        jitrun = Georeference_without_gcps(self.config,self.psql_conn)
        jitrun.run()
    
    def fix_gaps(self):
        sql = '''
        drop table if exists {schema_name}.filling_narrow_sections;
        create table {schema_name}.filling_narrow_sections
        as
        select 
            st_difference(
                st_makepolygon(
                    st_exteriorring(
                        st_buffer(
                            st_buffer(
                                st_union(geom),
                                10
                            ),
                            -10
                        )
                    )
                ),
                st_union(geom)
            ) as geom 
        from {input_table};
            
        drop table if exists {schema_name}.new_narrow_sections;
        create table {schema_name}.new_narrow_sections 
        as 
        select 
            st_buffer(
                st_buffer(
                    (st_dump(st_polygonize(geom))).geom,
                    -0.2),
                    0.2
                ) 
        as geom from {schema_name}.filling_narrow_sections;
            
        delete from {schema_name}.new_narrow_sections 
        where st_area(geom)<100; 
            
        alter table {schema_name}.new_narrow_sections 
        add column if not exists id serial;
            
        insert into {input_table} (survey_no,geom)
        select ('NN' || id ),st_multi(geom) from {schema_name}.new_narrow_sections;

        alter table {input_table}
        drop column if exists gid;
            
        alter table {input_table}
        add column gid serial;
            
        
        '''.format(input_table=self.survey_jitter ,
                   schema_name=self.schema_name)
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)


    def georef_using_gcps(self):
        tri = Trijunctions(self.config, self.psql_conn)
        tri.run()
        gcpmap = GCP_map(self.config,self.psql_conn)
        gcpmap.run()
        gcps_used_jitter = []
        gcps_used_polynomial_1 = []
        gcps_used_polynomial_2 = []
        gcps_used_projective = []
        gcps_used_spline = []
        poly1 = Polynomial(self.config, self.psql_conn)
        poly2 = Polynomial(self.config, self.psql_conn)
        print("\n-----Georeferencing Using Polynomial 1-----")
        deg_1 , gcps_used_polynomial_1 = poly1.run(1)
        print("\n-----Georeferencing Using Polynomial 2-----")
        deg_2 , gcps_used_polynomial_2 = poly2.run(2)
        print("\n-----Georeferencing Using Projective-----")
        proj = 0
        try:
            projective = Projective(self.config, self.psql_conn)
            proj, gcps_used_projective = projective.run()
        except:
            proj = math.inf
        print("\n-----Georeferencing Using Spline-----")
        spline = 0
        try:
            spl = Spline(self.config, self.psql_conn)
            spline, gcps_used_spline = spl.run()
        except:
            spline = math.inf
        
        xs_area_jitter = excess_area_at_boundary(self.psql_conn, self.schema_name, [0, 1, 1, 0, 0], self.survey_jitter, self.farmplots)
        
        excess_areas = [
            [xs_area_jitter, self.survey_jitter, "Jitter", gcps_used_jitter],
            [deg_1, poly1.survey_jitter_degree, "Polynomial 1", gcps_used_polynomial_1],
            [deg_2, poly2.survey_jitter_degree, "Polynomial 2", gcps_used_polynomial_2],
            [proj, projective.survey_projective, "Projective", gcps_used_projective],
            [spline, spl.survey_spline, "Spline", gcps_used_spline]
        ]

        excess_areas = list(filter(lambda x: x[0] != math.inf and x[0] != 0, excess_areas))

        excess_area_percentage = []
        for excess_list in excess_areas:
            excess_area_ratio = excess_list[0] / get_farmplot_areas_at_boundary(self.psql_conn, self.schema_name, excess_list[1], self.farmplots)
            excess_area_percentage.append([excess_list[0], excess_list[1], excess_list[2], excess_list[3], excess_area_ratio])
        
        excess_area_percentage.sort(key=lambda x: x[4])
        if len(excess_area_percentage) == 0:
            print("No georeferencing possible using gcps")
            return
        
        self.final_gcps_used.extend(excess_area_percentage[0][3])
        self.final_output[0] = excess_area_percentage[0][2]
        
        
        self.copy_table(excess_area_percentage[0][1], self.survey_georeferenced)
        self.validate_geom(self.survey_georeferenced)

        print("Final Georeferenced Output Selected :-", excess_area_percentage[0][2],
     
         "with excess area percent", 100*excess_area_percentage[0][0]/get_farmplot_areas_at_boundary(self.psql_conn, self.schema_name, self.survey_georeferenced, self.farmplots))

        
    
    def report(self):
        pass
    
    def run(self):
        self.jitter()
        self.fix_gaps()
        self.georef_using_gcps()
    
if __name__=="__main__":
    georef = georeferencer()
    georef.run()
    