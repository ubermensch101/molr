from config import *
from utils import *
from scripts import *
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create a FileHandler to log messages to a file
file_handler = logging.FileHandler('logfile.log')  # Specify the file name
file_handler.setLevel(logging.DEBUG)  # Set the logging level for this handler
file_handler.setFormatter(formatter)
# Add the FileHandler to the logger
logger.addHandler(file_handler)

def cut_narrow_faces(config, psql_conn, input_table, output_table):
    '''input table -> farm graph polygons
    output table'''
    try:
        narrow_faces_var = config.setup_details['pos']['narrow_faces']
        village = config.setup_details['setup']['village']
        narrow_faces = f'{village}.{narrow_faces_var}'

        sql_query = f'''
            drop table if exists {output_table};
            create table {output_table}
            as select * from {input_table};
            
            select 
                gid 
            from 
                {input_table}  
            ;
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
            res = curr.fetchall()
            
        for (gid,) in res:
            logger.info(f"Processing {input_table} gid {gid}")
            
            sql_query = f'''
                drop table if exists {village}.temp;
                create table {village}.temp as
                select 
                    (st_dump(
                        st_split(
                            (select geom from {input_table} where gid = {gid}),
                            st_boundary((select st_union(geom) from {narrow_faces}))
                        )
                    )).geom as geom,
                    (select type from {input_table} where gid = {gid}) as type
            '''
            with psql_conn.connection().cursor() as curr:
                curr.execute(sql_query)
            sql_query = f'''
                insert into {output_table} (geom,type)
                    select geom,type from {village}.temp;
                delete from {output_table}
                where gid = {gid};
            '''
            with psql_conn.connection().cursor() as curr:
                curr.execute(sql_query)
        sql_query = f'''
            alter table {output_table}
            drop column gid;
            alter table {output_table}
            add column gid serial;
        '''
        with psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)
        logger.info("finished cutting narrow faces")
    except:
        logger.error("error in cutting_narrow_faces", exc_info=True)



if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=True, default="")
    
    argument = parser.parse_args()
    path_to_data = argument.path
    village = argument.village
    shifted_faces = f'{village}.shifted_faces'
        
    config = Config()
    pgconn = PGConn(config)

    input_table = config.setup_details['pos']['input_table']
    output_table = config.setup_details['pos']['output_table']
    
    if village!="":
        config.setup_details['setup']['village'] = village
    
    cut_narrow_faces(config, pgconn, input_table, output_table)