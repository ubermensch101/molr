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
            
        
        '''.format(input_table=self.config.setup_details['setup']['village']+"."+ self.config.setup_details['data']['survey_jitter_table'],
                   schema_name=self.config.setup_details['setup']['village'])
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def label_gcps(self):
        pass


    def georef_using_gcps(self):
        tri = Trijunctions(self.config, self.psql_conn)
        tri.run()
        gcp_label_toggle = self.config.setup_details['georef']['gcp_label_toggle']
        if gcp_label_toggle == 'True':
            self.create_gcp_map_using_labels()
        elif gcp_label_toggle == 'False':
            self.create_gcp_map()

        
    
    def report(self):
        pass
    
    def run(self):
        self.jitter()
        self.fix_gaps()
    
if __name__=="__main__":
    georef = georeferencer()
    georef.run()
    