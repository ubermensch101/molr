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
        self.survey_no_label = config.setup_details['val']['survey_no_label']
        
    def fix_null_survey_number(self, table):
        sql = f'''
            with numbered_rows as (
            select
                gid,
                row_number() over (order by {self.survey_no_label}) as row_num
                from {table}
                where {self.survey_no_label} is NULL
            )
            update {table} as s
            set {self.survey_no_label} = 'n' || nr.row_num
            from numbered_rows as nr
            where s.{self.survey_no_label} is NULL
            and s.gid = nr.gid;
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    def merge_common_survey_number(self, table):
        sql = f'''
            with merged as (
                SELECT {self.survey_no_label}, st_multi((st_dump(ST_Union(geom))).geom) AS geom
                FROM {table}
                GROUP BY {self.survey_no_label}
            )
            UPDATE {table} a
            SET geom = b.geom
            FROM merged AS b
            WHERE b.{self.survey_no_label} = a.{self.survey_no_label}
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
                SELECT {self.survey_no_label}, COUNT(*) AS count
                FROM {table}
                GROUP BY {self.survey_no_label}
            )
            UPDATE {table} a
            SET valid = (sc.count = 1)
            FROM survey_counts sc
            WHERE a.{self.survey_no_label} = sc.{self.survey_no_label};
        '''
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            
    def fix_overlaps(self, schema, table):
        print("\n----------FIXING OVERLAPS IN SURVEY PLOTS----------")
        interections = list_overlaps(self.psql_conn, schema, table, 'gid')
        for gid1, gid2, _ in interections:
            sql = f'''
                with gid2 as (
                    select
                        geom as geom
                    from 
                        {schema}.{table}
                    where
                        gid = {gid2}
                )
                update {schema}.{table}
                set geom = st_multi(st_difference(geom, (select geom from gid2)))
                where
                    gid = {gid1}
            '''
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql)
            
            print(f'Fixed gid {gid1},{gid2} intersection by subtracting {gid2} from {gid1}')
            
    def run(self):
        copy_table(self.psql_conn, f'{self.village}.{self.survey}',f'{self.village}.{self.survey_processed}')
        self.fix_null_survey_number(self.village + '.' + self.survey_processed)
        self.merge_common_survey_number(self.village + '.' + self.survey_processed)
        self.mark_valid(self.village + '.' + self.survey_processed)
        self.fix_overlaps(self.village, self.survey_processed)
        if table_exist(self.psql_conn,self.village, self.akarbandh):
            add_akarbandh(self.psql_conn, 
                        self.village + '.' + self.survey_processed, 
                        self.village + '.' + self.akarbandh,
                        self.config.setup_details['data']['survey_map_akarbandh_col'],
                        self.config.setup_details['val']['survey_no_label'])
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()
    
    village = argument.village
    
    datacorrecter = data_correcter()
    datacorrecter.run()