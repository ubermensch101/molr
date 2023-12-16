from config import *
from utils import *
import argparse
import pandas as pd
import re


def analyse_gcps(config, psql_conn):
    delimiter = "-"
    schema = config.setup_details['setup']['village']
    gcp = config.setup_details['data']['gcp_table']
    gcp_label_column = config.setup_details['georef']['gcp_label_column']
    gcp_table = schema + "." + gcp
    gcp_report = config.setup_details['georef']['gcp_report']
    gcp_report_table = schema + "." + gcp_report
    if not check_column_exists(psql_conn, schema , gcp, gcp_label_column ):
        config.setup_details['georef']['gcp_label_toggle']= "False"
    else:
        config.setup_details['georef']['gcp_label_toggle']= "True"
        sql = f'''
            drop table if exists {gcp_report_table};
            create table {gcp_report_table} as
            select gid, {gcp_label_column} from {gcp_table};
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        sql = f'''
            alter table {gcp_report_table}
            add column if not exists Parseable varchar(100); 
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
        sql = f''' select {gcp_label_column} from {gcp_table}; '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            gcp_labels = curr.fetchall()
        for gcp_label in gcp_labels:
            survey_nos = re.split(delimiter,gcp_label[0])
            flag = "Yes"
            for survey_no in survey_nos:
                try:
                    a = int(survey_no)
                    flag = "Yes"
                except:
                    if survey_no in ['vb' , 'rv', 'rd', 'g']:
                        flag = "Yes"
                    else:
                        flag = "No"
            sql = f'''
                update {gcp_report_table}
                set Parseable = '{flag}'
                where {gcp_label_column} = '{gcp_label[0]}' ;
            '''
            with psql_conn.connection().cursor() as curr:
                curr.execute(sql)
        



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
    
    analyse_gcps(config, pgconn)
