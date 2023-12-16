import subprocess
import os
import argparse
from config import *

def get_bounds(config,path):
    village = config.setup_details['setup']['village']
    host = config.setup_details['psql']['host']
    port = config.setup_details['psql']['port']
    database = config.setup_details['psql']['database']
    user = config.setup_details['psql']['user']
    password = config.setup_details['psql']['password']
    cadastrals = config.setup_details['data']['cadastrals_table']
    
    if not os.path.exists(path):
        os.mkdir(path)
        
    file = os.path.join(path, f'{village}_bounds.kml')
        
    ogr2ogr_cmd = [
        'ogr2ogr','-f','KML',f'{file}',
        'PG:dbname=' + database + ' host=' + host + ' user=' + user +' password=' + password + ' port=' + port,
        '-sql',
        f'select st_transform(st_expand(st_transform(st_union(geom),32643),150),4326) AS geom FROM {village}.{cadastrals}'
    ]
    subprocess.run(ogr2ogr_cmd)

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
    
    get_bounds(config, path)
    
    