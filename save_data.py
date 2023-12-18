import subprocess
import os
import argparse
import pandas as pd
from config import *
from utils import PGConn

def save_data(config,path):
    
    village = config.setup_details['setup']['village']
    host = config.setup_details['psql']['host']
    port = config.setup_details['psql']['port']
    database = config.setup_details['psql']['database']
    user = config.setup_details['psql']['user']
    password = config.setup_details['psql']['password']
    survey_georef = config.setup_details['data']['survey_georeferenced_table']
    shifted = config.setup_details['data']['shifted_faces_table']
    possession = config.setup_details['data']['possession_table']
    report = config.setup_details['val']['report_table']
    gcp_report = config.setup_details['georef']['gcp_report']
    farmplots = config.setup_details['data']['farmplots_table']
        
    village_folder_name = os.path.join(path,village)
    if not os.path.exists(village_folder_name):
        os.makedirs(village_folder_name)
        print(f'Creating directory {village_folder_name}')
    for table in [survey_georef,shifted,possession,farmplots]:
        table_folder = os.path.join(village_folder_name, table)
        file = os.path.join(table_folder, f'{village}_{table}.shp')
        
        if not os.path.exists(table_folder):
            os.makedirs(table_folder)
        
        ogr2ogr_cmd = [
            'ogr2ogr','-f','ESRI Shapefile',f'{file}',
            'PG:dbname=' + database + ' host=' + host + ' user=' + user +' password=' + password + ' port=' + port,
            '-sql',
            f'select * FROM {village}.{table}'
        ]
        subprocess.run(ogr2ogr_cmd)
        
    for table in [report, gcp_report]:
        file = os.path.join(village_folder_name, f'{village}_{table}.csv')
        psql_conn = PGConn(config)
        sql_query = f'''
            select * from {village}.{table};
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            columns = [d[0] for d in curr.description]
            data = curr.fetchall()
        df = pd.DataFrame(data,columns = columns)
        df.to_csv(file, index=False)
        
    zip_cmd = [
        'zip','-r',
        f'{village_folder_name}.zip',
        f'{village_folder_name}'
    ]  
    subprocess.run(zip_cmd)  

if __name__=="__main__":
    
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-p", "--path", help="Path to folder to save",
                        required=True, default="")
    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")
    
    argument = parser.parse_args()
    path = argument.path
    village = argument.village
    
    if path=="":
        print("ERROR, no data storing path")
        exit()
        
    config = Config()
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    save_data(config, path)
    
    