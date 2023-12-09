import os
import subprocess
import argparse
import pandas as pd
from utils import *
from config import *

def convert_row(row):
    try:
        row[0] = int(row[0])
        row[1] = str(row[1])
        row[2] = float(row[2])
        return row
    except (ValueError, TypeError):
        return None


def load_akarbandh_from_excel(psql_conn, path, village, table_name):
    df = pd.read_excel (path, usecols='A, B, C', dtype=str)
    df = df.apply(convert_row, axis=1).dropna()
    conn = psql_conn.connection()
    new_df = df.dropna()
    akarbandh_values = new_df.values.tolist()
    sql = f"""
        drop table if exists {village}.{table_name};
        create table {village}.{table_name} (
            gid serial, 
            survey_no varchar(20), 
            area float
        );
    """
    with conn.cursor() as curr:
        curr.execute(sql)
        curr.executemany(f"INSERT INTO {village}.{table_name} VALUES(%s,%s,%s)", akarbandh_values)
        
    # not needed
    sql = f'''
    UPDATE {village}.{table_name} as p
        SET area = b.area
        FROM (
            SELECT survey_no, sum(area) AS area
            FROM {village}.{table_name}
            where area>0
            GROUP BY survey_no
        ) AS b
        WHERE b.survey_no = p.survey_no
    '''
    with conn.cursor() as curr:
        curr.execute(sql)
    sql = f'''
    DELETE FROM {village}.{table_name}
    WHERE gid IN
        (SELECT gid
        FROM 
            (SELECT gid,
            ROW_NUMBER() OVER( PARTITION BY survey_no
            ORDER BY  gid ) AS row_num
            FROM {village}.{table_name} ) t
            WHERE t.row_num > 1 );
    '''
    with conn.cursor() as curr:
        curr.execute(sql)
    

def load_akarbandh(config, psql_conn, path_to_akarbandh):
    for root, dirs, files in os.walk(path_to_akarbandh, topdown=True):
        for file in files:
            file_location = os.path.join(root, file)
            table_name = config.setup_details['data']['akarbandh_table']
            survey_original =  config.setup_details['data']['survey_map_table']
            village_name = config.setup_details['setup']['village']
            if file.endswith(".xlsx"):
                load_akarbandh_from_excel(psql_conn, file_location,village_name,table_name)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-p", "--path", help="Path to data",
                        required=True, default="")
    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
    
    if path_to_data=="" or village=="":
        print("ERROR")
        exit()
        
    config = Config()
    pgconn = PGConn(config)
    
    config.setup_details['setup']['village'] = village
    
    load_akarbandh(config,pgconn,path_to_data)