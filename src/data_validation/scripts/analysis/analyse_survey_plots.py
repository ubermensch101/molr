from config import *
from utils import *
import argparse
from collections import Counter

def analyse_survey_plots(config, psql_conn):
    survey = config.setup_details['data']['survey_map_table']
    akarbandh = config.setup_details['data']['akarbandh_table']
    village = config.setup_details['setup']['village']
    
    print("\n----------SURVEY PLOTS----------")
    if table_exist(psql_conn,village,survey):
        print("Survey plots table exists!")
    else:
        print("Survey plots table does not exist!")
        return
    
    sql = f'''
        select
            (survey_no)
        from
            {village}.{survey};    
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        res = curr.fetchall()
    survey_numbers = []
    for result in res:
        survey_numbers.append(str(result[0]))

    
    print("Total number of survey plots:", len(survey_numbers))
    print("List of repeating survey numbers:",end=" ")
    
    element_counts = Counter(survey_numbers)
    
    # repeated_elements = [element for element, count in element_counts.items() if count > 1]
    frequencies = {element: count for element, count in element_counts.items() if count > 1}

    if(len(frequencies) == 0):
        print("None")
    else:
        print("\nSurvey no    Number of times repeated")
        for i in frequencies:
            print(i, "          ", frequencies[i])

    print("Non integer Survey numbers:", end=" ")

    acutal_survey_no_list = []
    if(len(survey_numbers) == 0):
        print("None")
    else:
        for i in survey_numbers:
            if i is not None:
                if  not i.isnumeric():
                    print(i, end="  ")
                else:
                    acutal_survey_no_list.append(i)
            else:
                print("None",end=' ')
                
    if not table_exist(psql_conn, village, akarbandh):
        return
    
    print("\nList of missing survey numbers:",end=" ")
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
        
    for i in range(1,len(ror_list)+1):
        if i not in acutal_survey_no_list and i in ror_list :
            print(i,end='  ')
    
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
    
    analyse_survey_plots(config, pgconn)