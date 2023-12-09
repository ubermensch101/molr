from config import *
from utils import *
import argparse

def analyse_akarbandh(config, psql_conn):
    survey = config.setup_details['data']['survey_map_table']
    akarbandh = config.setup_details['data']['akarbandh_table']
    village = config.setup_details['setup']['village']
    
    print("\n----------Akarbandh----------")
    if table_exist(psql_conn, village, akarbandh):
        print("Akarbandh table exists!")
    else:
        print("Akarbandh table does not exist!")
        return 
    
    sql = f'''
        select count(survey_no),sum(area) from {village}.{akarbandh};
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
    count = res[0][0]
    area = res[0][1]
    
    sql = f'''
        select 
            (survey_no)
        from 
            {village}.{akarbandh};
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
    ror_list = []
    for result in res:
        ror_list.append(str(result[0]))
    
    print("Total entries in Akarbandh:", count)
    print("Total Akarbandh area:", area, "Ha")
    
    sql = f'''
        select
            (survey_no :: int)
        from
            {village}.{survey}
        where
            survey_no ~ '^[0-9\.]+$'
        order by
            survey_no;     
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
    survey_numbers = []
    for result in res:
        survey_numbers.append(str(result[0]))

    print("Missing Survey numbers in Akarbandh:", end=" ")
    miss = []
    for i in survey_numbers:
        if i not in ror_list:
            miss.append(i)
    res = [*set(miss)]
    if(len(res)==0):
        print("None")
    else:
        print(res,sep=" ")
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
        
    config = Config()
    pgconn = PGConn(config)
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    analyse_akarbandh(config, pgconn)
