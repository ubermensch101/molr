from config import *
from utils import *
import argparse

def analyse_cadastrals(config, psql_conn):
    cadastrals = config.setup_details['data']['cadastrals_table']
    village = config.setup_details['setup']['village']
    
    print("\n----------CADASTRALS----------")
    if table_exist(psql_conn,village,cadastrals):
        print("Cadastral table exists!")
    else:
        print("Cadastral table does not exist!")
        return
    print("Total number of Cadastrals:", number_of_entries(psql_conn, village, cadastrals))
    
    if check_column_exists(psql_conn,village,cadastrals,'pin'):
        comment = ""
    else:
        comment = "--"
    
    sql = f'''
        select 
            sum(st_area(geom)) 
        from 
            {village}.{cadastrals};
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
    area = round(float(res[0][0])/10000, 2)
    print("Total area of cadastrals:", area, "Ha")
    
    sql = f'''
        select 
            count(*)
        from 
            {village}.{cadastrals}
            
        {comment}where pin is NULL
        
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
    count = int(res[0][0])
    print("Number of NULL cadastrals:", count)
    sql = f'''
        select 
            sum(st_area(geom)) 
        from 
            {village}.{cadastrals}
        {comment}where
        {comment}    pin is not null
        ;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
        
    non_null_area = round(float(res[0][0])/10000, 2)
    print("Total non-NULL area: ",non_null_area, " Ha")
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")
    
    argument = parser.parse_args()
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    analyse_cadastrals(config, pgconn)
