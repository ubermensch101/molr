from config import *
from utils import *
import pandas as pd
import argparse
import json

def add_summary(config, psql_conn, path=""):
    village = config.setup_details['setup']['village']
    if path != "":
        df = pd.read_csv(path)
    else:
        report = config.setup_details['val']['report_table']
        
        with psql_conn.connection().cursor() as curr:
            curr.execute(f"select * from {village}.{report}")
            column_names = [desc[0] for desc in curr.description]
            values = curr.fetchall()
        
        df = pd.DataFrame(values, columns = column_names)
        
    get_summary(config, psql_conn, df)

def get_summary(config, psql_conn, df):
    village = config.setup_details['setup']['village']
    thresh = int(config.setup_details['val']['validation_thresh'])
    fr_thresh = int(config.setup_details['val']['farm_rating_validation_thresh'])
    
    df2 = df.dropna()
    survey_plots = df.shape[0]
    valid = len(df2)
    akarbandh_validity = len(df2[df2['akarbandh_validity']])
    non_int = df[~df['survey_no'].astype(str).str.isdigit()].shape[0]
    
    geo_with_akar = 100*len(df2[abs(df2['geo_with_akarbandh']) < thresh])/valid
    ref_with_akar = 100*len(df2[abs(df2['ref_with_akarbandh']) < thresh])/valid
    pos_with_akar = 100*len(df2[abs(df2['pos_with_akarbandh']) < thresh])/valid

    ref_geo_area_perimeter = 100*len(df2[(abs(df2['ref_with_geo_area'])<thresh) & (abs(df2['ref_with_geo_perimeter'])<thresh)])/valid
    ref_geo_deviation = 100*len(df2[abs(df2['ref_with_geo_deviation'])<thresh])/valid
    ref_geo_apd = 100*len(df2[(abs(df2['ref_with_geo_area'])<thresh) & (abs(df2['ref_with_geo_perimeter'])<thresh) & (abs(df2['ref_with_geo_deviation'])<thresh)])/valid
    ref_geo_ad = 100*len(df2[(abs(df2['ref_with_geo_area'])<thresh) & (abs(df2['ref_with_geo_deviation'])<thresh)])/valid
    
    pos_geo_area_perimeter = 100*len(df2[(abs(df2['pos_with_geo_area'])<thresh) & (abs(df2['pos_with_geo_perimeter'])<thresh)])/valid
    pos_geo_deviation = 100*len(df2[abs(df2['pos_with_geo_deviation'])<thresh])/valid
    pos_geo_apd = 100*len(df2[(abs(df2['pos_with_geo_area'])<thresh) & (abs(df2['pos_with_geo_perimeter'])<thresh) & (abs(df2['pos_with_geo_deviation'])<thresh)])/valid
    pos_geo_ad = 100*len(df2[(abs(df2['pos_with_geo_area'])<thresh) & (abs(df2['pos_with_geo_deviation'])<thresh)])/valid
    
    geo_farm_rating = 100*len(df2[df2['geo_farm_rating'] > fr_thresh])/valid
    ref_farm_rating = 100*len(df2[df2['ref_farm_rating'] > fr_thresh])/valid
    pos_farm_rating = 100*len(df2[df2['pos_farm_rating'] > fr_thresh])/valid
    
    result_row = {
        'village': village,
        'survey_plots': survey_plots,
        'non_int': non_int,
        'valid': valid,
        'akarbandh_validity':akarbandh_validity,
        
        'geo_with_akar': geo_with_akar,
        'geo_farm_rating':geo_farm_rating,
        
        'ref_with_akar': ref_with_akar,
        'ref_with_geo_ap': ref_geo_area_perimeter,
        'ref_with_geo_d': ref_geo_deviation,
        'ref_with_geo_apd': ref_geo_apd,
        'ref_with_geo_ad': ref_geo_ad,
        'ref_farm_rating':ref_farm_rating,
        
        'pos_with_akar': pos_with_akar,
        'pos_with_geo_ap': pos_geo_area_perimeter,
        'pos_with_geo_d': pos_geo_deviation,
        'pos_with_geo_apd': pos_geo_apd,
        'pos_with_geo_ad': pos_geo_ad,
        'pos_farm_rating':pos_farm_rating
    }
    print(json.dumps(result_row, indent = 4))

    summary_table = config.setup_details['val']['summary_table']
    entry = (village, survey_plots, non_int, valid,akarbandh_validity,
         geo_with_akar,geo_farm_rating,
         ref_with_akar, ref_geo_area_perimeter, ref_geo_deviation, ref_geo_apd, ref_geo_ad,ref_farm_rating,
         pos_with_akar, pos_geo_area_perimeter, pos_geo_deviation, pos_geo_apd, pos_geo_ad,pos_farm_rating)
    
    sql = f"""
        create table if not exists {summary_table}
        (
            village varchar(100),
            survey_plots int,
            non_int int,
            valid int,
            akarbandh_validity int,
            geo_with_akar float,
            geo_farm_rating float,
            ref_with_akar float,
            ref_with_geo_ap float,
            ref_with_geo_d float,
            ref_with_geo_apd float,
            ref_with_geo_ad float,
            ref_farm_rating float,
            pos_with_akar float,
            pos_with_geo_ap float,
            pos_with_geo_d float,
            pos_with_geo_apd float,
            pos_with_geo_ad float,
            pos_farm_rating float
        );
    
        delete from {summary_table}
        where village = '{village}';
        
        insert into {summary_table} 
        values
            {str(entry)};
    """
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-p", "--path", help="Path to csv",
                        required=False, default="")
    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
    
    if village=="" and path_to_data != "":
        print("ERROR")
        exit()
        
    config = Config()
    pgconn = PGConn(config)
    
    config.setup_details['setup']['village'] = village
    
    add_summary(config, pgconn, path_to_data)