from config import *
from utils import *
import argparse

def data_cleaner(village=""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Data_Cleaner(config,pgconn)

class Data_Cleaner:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.village = self.config.setup_details['setup']['village']
        self.gcp = self.config.setup_details['data']['gcp_table']
        self.survey = self.config.setup_details['data']['survey_map_table']
        self.gcp_label = self.config.setup_details['val']['gcp_label']
        self.survey_label = self.config.setup_details['val']['survey_no_label']
        self.snap_tol = self.config.setup_details['val']['snap_tol']
        if self.village == "":
            print("ERROR")
            exit()
        
    def process_gcps(self):
        srid = find_srid(self.psql_conn, self.village,self.gcp,'geom')
        if srid != 32643:
            print(f"GCP SRID error, found {srid} instead of 32643")
        add_column(self.psql_conn, self.village+'.'+self.gcp, 'gid','serial')
        add_column(self.psql_conn, self.village+'.'+self.gcp, 'geom2','geometry(Point,32643)')
        sql = f'''
            update {self.village}.{self.gcp} as p
            set geom2 = (select (st_dump(st_force2D(geom))).geom  from {self.village}.{self.gcp} where gid = p.gid);

            alter table {self.village}.{self.gcp}
            drop column geom;

            alter table {self.village}.{self.gcp}
            rename column geom2 to geom;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        if not check_column_exists(self.psql_conn, self.village,self.gcp,self.gcp_label):
            print(f"Column {self.gcp_label} does not exists in GCP")
            
    def process_survey_original(self):
        if not check_column_exists(self.psql_conn, self.village, self.survey, self.survey_label):
            print(f"Column {self.survey_label} does not exists in Village map")
        sql = f'''
            select UpdateGeometrySRID('{self.village}','{self.survey}','geom','32643')
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        add_column(self.psql_conn,self.village+'.'+self.survey,'gid','serial')
        add_column(self.psql_conn,self.village+'.'+self.survey,'geom2','geometry(Multipolygon,32643)')
        sql = f'''
            update {self.village}.{self.survey}
            set geom2 =  st_force2D(st_multi(geom));
            alter table {self.village}.{self.survey}
            drop column geom;
            alter table {self.village}.{self.survey}
            rename geom2 to geom;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        # to correct: 
        # psycopg2.errors.InternalError_: SQL/MM Spatial exception - geometry crosses edge 13
        self.clean_snap_error(f'{self.village}.{self.survey}')
    
    def clean_snap_error(self, survey_map):
        sql = f'''
        update 
            {survey_map} 
        set geom = 
        (select 
            st_multi(
                st_snap(
                    geom,
                    (select st_collect(geom) from {survey_map}), 
                    {self.snap_tol}
                )
            )
        );
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def run(self):
        self.process_gcps()
        self.process_survey_original()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    datacleaner = data_cleaner(village)
    datacleaner.run()

