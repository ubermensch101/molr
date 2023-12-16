from .create_gcp_map import *
from .find_tri_junctions import *


def gcp_trijunction_match(config, psql_conn):
    schema = config.setup_details['setup']['village']
    gcp = config.setup_details['data']['gcp_table']
    gcp_table = schema + "." + gcp
    gcp_label_column = config.setup_details['georef']['gcp_label_column']
    gcp_report = config.setup_details['georef']['gcp_report']
    gcp_report_table = schema + "." + gcp_report
    sql = f'''
            alter table {gcp_report_table}
            add column if not exists Survey_vertex_match varchar(100); 
            alter table {gcp_report_table}
            add column if not exists jitter_distance varchar(100); 
        '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
    gcp_label_toggle = config.setup_details['georef']['gcp_label_toggle']
    if gcp_label_toggle == "False":
        return
    elif gcp_label_toggle == "True":
        obj = Trijunctions(config, psql_conn)
        obj.find_tri_junctions(obj.survey_jitter, obj.schema_name+"_"+obj.survey_jitter+"_topo", obj.survey_jitter_vertices )
        survey_jitter_vertices = schema + "." + obj.survey_jitter_vertices
        sql = f'''
            select st_astext(geom) , label from {survey_jitter_vertices} ;
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            jitter_vertices = curr.fetchall()
        sql = f'''
            select st_astext(geom) , {gcp_label_column} ,gid from {gcp_table} ;
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql)
            gcp_vertices = curr.fetchall()
        distance_list = []
        for gcpdata in gcp_vertices:
            flag = "Not Found"
            distance = ""
            for vertex in jitter_vertices:
                if (set(gcpdata[1].split('-')) == set(vertex[1].split('-'))):
                    flag = "Found"
                    sql = f'''
                        select st_distance(st_geomfromtext('{gcpdata[0]}'),st_geomfromtext('{vertex[0]}'));
                    '''
                    with psql_conn.connection().cursor() as curr:
                        curr.execute(sql)
                        distance = curr.fetchall()
                    distance = distance[0][0]
                    distance_list.append(float(distance))
                    break
            sql = f'''
                update {gcp_report_table}
                set Survey_vertex_match = '{flag}' , jitter_distance = '{distance}' 
                where gid = {gcpdata[2]} ;
            '''
            with psql_conn.connection().cursor() as curr:
                curr.execute(sql)
        sum = 0
        for dist in distance_list:
            sum = sum + dist**2
        rms = np.sqrt(sum/len(distance_list))
        print('Root Mean Squared Distance :' , rms)
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")
    parser.add_argument("-gcp_toggle", "--gcp_label_toggle", help="GCP label column exists?",
                        required=False, default="")
    argument = parser.parse_args()
    village = argument.village
    gcp_toggle = argument.gcp_label_toggle   
    config = Config()
    pgconn = PGConn(config)
    
    if village!="":
        config.setup_details['setup']['village'] = village
    if gcp_toggle != "":
        config.setup_details['georef']['gcp_label_toggle'] = gcp_toggle
    
    gcp_trijunction_match(config, pgconn)
