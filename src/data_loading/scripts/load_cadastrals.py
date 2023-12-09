import os
import subprocess
import argparse
from utils import *
from config import *

def load_cadastrals(psql_conn, path_to_cadastrals, village_name):
    for root, dirs, files in os.walk(path_to_cadastrals, topdown=True):
        for file in files:
            file_location = os.path.join(root, file)
            table_name = "cadastrals"
            if file.endswith(".shp"):
                ogrinfo_cmd = [
                    'ogrinfo',
                    '-q',
                    file_location
                ]
                output = subprocess.check_output(ogrinfo_cmd, universal_newlines=True)
                if 'Polygon' in output:
                    ogr2ogr_cmd = [
                        'ogr2ogr','-f','PostgreSQL',
                        'PG:dbname=' + psql_conn.details["database"] + ' host=' +
                            psql_conn.details["host"] + ' user=' + psql_conn.details["user"] +
                            ' password=' + psql_conn.details["password"],
                        file_location,
                        '-lco', 'OVERWRITE=YES',
                        '-lco', 'GEOMETRY_NAME=geom',
                        '-lco', 'schema=' + village_name, 
                        '-lco', 'SPATIAL_INDEX=GIST',
                        '-lco', 'FID=gid',
                        '-nlt', 'PROMOTE_TO_MULTI',
                        '-nln', table_name
                    ]
                    subprocess.run(ogr2ogr_cmd)


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
    config = Config()
    pgconn = PGConn(config)
    
    # try:
    load_cadastrals(pgconn,path_to_data,village)
    # except:
    #     print("ERROR 2")