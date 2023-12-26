from scripts import *
from utils import *
from config import *
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

def setup_pos(village=""):
    config = Config()
    pgconn = PGConn(config)
    
    return Setup_Pos(config,pgconn)

class Setup_Pos:
    def __init__(self, config, psql_conn):
        self.config = config
        self.psql_conn = psql_conn
        self.narrow_faces_table = self.config.setup_details['pos']['narrow_faces']
        self.narrow_faces_input = self.config.setup_details['pos']['shifted_faces']
        self.village = self.config.setup_details['setup']['village']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']

    def narrow_face_identifier(self):
        try:
            village = self.config.setup_details['setup']['village']
            shifted_faces = self.config.setup_details['pos']['shifted_faces']
            narrow_faces = self.config.setup_details['pos']['narrow_faces']
            sql_query=f"""
                drop table if exists {village}.{narrow_faces};
                create table {village}.{narrow_faces} as

                select geom                
                from
                    {village}.{shifted_faces}
                where
                    st_area(geom)>1
                    and
                    st_perimeter(geom) * 
                        st_perimeter(geom) /
                        st_area(geom) > 55
                ;
            """
            with self.psql_conn.connection().cursor() as curr:
                curr.execute(sql_query)
            self.psql_conn.connection().commit()
            logger.info("Identified narrow faces")
        except:
            logger.error("Error identifying narrow faces", exc_info=True)

    def add_void_polygon(self):
        sql_query = f"""
            with un as (
                select st_union(geom) as geom from {self.village}.{self.farm_faces}
            ),
            g as (
                select st_difference(st_envelope(un.geom),un.geom) as geom from un
            )
            
            insert into {self.village}.{self.farm_faces} (geom)
            select geom from g;
        """
        with self.psql_conn.connection().cursor() as curr:
            curr.execute(sql_query)

    def run(self):
        self.narrow_face_identifier()
        add_column(self.psql_conn, f'{self.village}.{self.narrow_faces_input}', 'gid', 'serial')
        self.add_void_polygon()
        add_column(self.psql_conn, f'{self.village}.{self.farm_faces}', 'gid', 'serial')
