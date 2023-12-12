from config import *
from utils import *
import argparse

def data_correcter(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Survey_Map_Processer(config,pgconn)

class Survey_Map_Processer:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.survey = config.setup_details['data']['survey_map_table']
        self.village = config.setup_details['setup']['village']
        self.survey_processed = config.setup_details['data']['survey_processed']
        self.akarbandh = config.setup_details['data']['akarbandh_table']
        
    def fix_null_survey_number(self, table):
        sql = f'''
            with numbered_rows as (
            select
                gid,
                row_number() over (order by survey_no) as row_num
                from {table}
                where survey_no is NULL
            )
            update {table} as s
            set survey_no = 'n' || nr.row_num
            from numbered_rows as nr
            where s.survey_no is NULL
            and s.gid = nr.gid;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    def merge_common_survey_number(self, table):
        sql = f'''
            with merged as (
                SELECT survey_no, st_multi((st_dump(ST_Union(geom))).geom) AS geom
                FROM {table}
                GROUP BY survey_no
            )
            UPDATE {table} a
            SET geom = b.geom
            FROM merged AS b
            WHERE b.survey_no = a.survey_no
            and st_intersects(b.geom,a.geom);
            
            with dup as (
                select 
                    a.gid as gid
                from 
                    {table} a,
                    {table} b
                where
                    a.geom = b.geom and
                    a.gid < b.gid
            )
            delete from {table} a
            where
                a.gid in (select gid from dup)
            ;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def mark_valid(self, table):
        sql = f'''
            alter table {table}
            add column if not exists valid bool;
            
            WITH survey_counts AS (
                SELECT survey_no, COUNT(*) AS count
                FROM {table}
                GROUP BY survey_no
            )
            UPDATE {table} a
            SET valid = (sc.count = 1)
            FROM survey_counts sc
            WHERE a.survey_no = sc.survey_no;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)

    def run(self):
        copy_table(self.psql_conn, f'{self.village}.{self.survey}',f'{self.village}.{self.survey_processed}')
        self.fix_null_survey_number(self.village + '.' + self.survey_processed)
        self.merge_common_survey_number(self.village + '.' + self.survey_processed)
        self.mark_valid(self.village + '.' + self.survey_processed)
        if table_exist(self.psql_conn,self.village, self.akarbandh):
            add_akarbandh(self.psql_conn, 
                        self.village + '.' + self.survey_processed, 
                        self.village + '.' + self.akarbandh,
                        self.config.setup_details['data']['survey_map_akarbandh_col'])
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    
    datacorrecter = Survey_Map_Processer()
    datacorrecter.run()