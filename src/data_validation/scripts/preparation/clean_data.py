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
        self.cadastrals = self.config.setup_details['data']['cadastrals_table']
        self.gcp_label = self.config.setup_details['val']['gcp_label']
        self.survey_label = self.config.setup_details['val']['survey_no_label']
        self.snap_tol = self.config.setup_details['val']['snap_tol']
        self.srid = config.setup_details['setup']['srid']
        if self.village == "":
            print("ERROR")
            exit()
        
    def process_gcps(self):
        
        print("\nCleaning GCPs")
        
        if not check_column_exists(self.psql_conn, self.village, self.gcp, 'gid'):
            print("Column GID doesnot exists, updating it")
            add_column(self.psql_conn, self.village+'.'+self.gcp, 'gid','serial')
            
        if not check_column_exists(self.psql_conn, self.village,self.gcp,self.gcp_label):
            print(f"Column {self.gcp_label} does not exists in GCP")
        
        curr_srid = find_srid(self.psql_conn, self.village,self.gcp,'geom')
        col_type = find_column_geom_type(self.psql_conn, self.village,self.gcp, 'geom')
        
        if curr_srid != int(self.srid) or col_type!="POINT":
            print(f"GCP SRID error, found srid {curr_srid} and type {col_type}, updating it")
            sql = f'''
                alter table {self.village}.{self.gcp}
                alter column geom type geometry({col_type}, {self.srid})
                using st_transform(geom,{self.srid});
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
            add_column(self.psql_conn, self.village+'.'+self.gcp, 'geom2', f'geometry(Point, {self.srid})')
            sql = f'''
                update table {self.village}.{self.gcp}
                set geom2 = st_force2d((st_dump(geom)).geom);
                
                alter table {self.village}.{self.gcp}
                drop column geom;
                
                alter table {self.village}.{self.gcp}
                rename column geom2 geom;
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
            
            
    def process_survey_original(self):
        
        print("\nCleaning Survey Map")
        
        if not check_column_exists(self.psql_conn, self.village, self.survey, 'gid'):
            print("Column GID doesnot exists, updating it")
            add_column(self.psql_conn, self.village+'.'+self.survey, 'gid','serial')
            
        if not check_column_exists(self.psql_conn, self.village, self.survey, self.survey_label):
            print(f"Column {self.survey_label} does not exists in Village map!!")
            
        curr_srid = find_srid(self.psql_conn, self.village,self.survey,'geom')
        col_type = find_column_geom_type(self.psql_conn, self.village, self.survey, 'geom')
        
        if curr_srid != int(self.srid) or col_type!="MULTIPOLYGON":
            print(f"Survey map error, found srid {curr_srid} and type {col_type}, Updating it")
            sql = f'''
                alter table {self.village}.{self.survey}
                alter column geom type geometry(MultiPolygon, {self.srid})
                using st_transform(st_force2d(st_multi(geom))), {self.srid});
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
        
        print("Cleaning Snap error")
        self.clean_snap_error(f'{self.village}.{self.survey}')
        
    def process_cadastrals(self):
        
        print("\nCleaning Cadastrals")
        
        if not check_column_exists(self.psql_conn, self.village, self.cadastrals, 'gid'):
            print("Column GID doesnot exists, updating it")
            add_column(self.psql_conn, self.village+'.'+self.cadastrals, 'gid','serial')
            
        if not check_column_exists(self.psql_conn, self.village, self.cadastrals, 'pin'):
            print(f"Column 'pin' does not exists in cadastral map")
            
        curr_srid = find_srid(self.psql_conn, self.village,self.cadastrals,'geom')
        col_type = find_column_geom_type(self.psql_conn, self.village, self.cadastrals, 'geom')
        
        if curr_srid != int(self.srid) or col_type!="MULTIPOLYGON":
            print(f"Cadastrals map error, found srid {curr_srid} and type {col_type}, Updating it")
            sql = f'''
                alter table {self.village}.{self.cadastrals}
                alter column geom type geometry(MultiPolygon, {self.srid})
                using st_transform(st_force2d(st_multi(geom))), {self.srid});
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
            
        print("Cleaning Snap error")
        self.clean_snap_error(f'{self.village}.{self.cadastrals}')
        
    def clean_snap_error(self, map):
        sql = f'''
        update 
            {map} 
        set geom = 
        (select 
            st_force2d(st_multi(
                st_snap(
                    geom,
                    (select st_collect(geom) from {map}), 
                    {self.snap_tol}
                )
            ))
        );
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def run(self):
        print("\n-------Cleaning Data--------")
        self.process_gcps()
        self.process_survey_original()
        self.process_cadastrals()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village

    datacleaner = data_cleaner(village)
    datacleaner.run()

