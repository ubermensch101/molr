from scripts import *
from utils import *
from config import *
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

def possession(village = ""):
    config = Config()
    
    pgconn = PGConn(config)
    if village != "":    
        config.setup_details['setup']['village'] = village
    
    return Possession(config,pgconn)

class Possession:
    def __init__(self,config,psql_conn):
        self.config = config
        self.village = self.config.setup_details['setup']['village']
        self.farm_faces = self.config.setup_details['pos']['farm_faces']
        self.shifted_faces = self.config.setup_details['pos']['shifted_faces']
        self.farm_topo = self.config.setup_details['pos']['farm_superpoly_topo']
        self.psql_conn = psql_conn

        self.farm_ownership = self.config.setup_details['pos']['ownership']
        self.farm_ownership_2 = self.config.setup_details['pos']['ownership_2']
        self.farm_ownership_3 = self.config.setup_details['pos']['ownership_3']
        self.farm_ownership_4 = self.config.setup_details['pos']['ownership_4']

        self.possession_1 = self.config.setup_details['pos']['possession_1']
        self.possession_2 = self.config.setup_details['pos']['possession_2']
        self.possession_3 = self.config.setup_details['pos']['possession_3']
        self.possession_4 = self.config.setup_details['pos']['possession_4']
        self.possession_5 = self.config.setup_details['pos']['possession_5']
        self.temp_possession = self.config.setup_details['pos']['temporary_possession']
        self.possession_final = self.config.setup_details['pos']['possession_final']
        self.shifted_topo = self.config.setup_details['pos']['topo']
        
    def cut_farms(self):
        possession_1 = f'{self.village}.{self.possession_1}'
        possession_2 = f'{self.village}.{self.possession_2}'
        possession_3 = f'{self.village}.{self.possession_3}'
        possession_4 = f'{self.village}.{self.possession_4}'
        temporary_possession = f'{self.village}.{self.temp_possession}'

        farm_plot_ownership = f'{self.village}.{self.farm_ownership}'
        farm_plot_ownership_2 = f'{self.village}.{self.farm_ownership_2}'
        farm_plot_ownership_3 = f'{self.village}.{self.farm_ownership_3}'
        farm_plot_ownership_4 = f'{self.village}.{self.farm_ownership_4}'
        
        try:
            cut_narrow_faces(self.config, self.psql_conn, farm_plot_ownership, temporary_possession)
            create_super_poly(self.config, self.psql_conn, self.shifted_faces, temporary_possession, possession_1)
            add_column(self.psql_conn, temporary_possession, 'gid', 'serial')
            break_voids(self.config, self.psql_conn, temporary_possession, farm_plot_ownership_2)
            create_super_poly(self.config, self.psql_conn, self.shifted_faces, farm_plot_ownership_2, possession_2)
            break_voids(self.config, self.psql_conn, farm_plot_ownership_2, farm_plot_ownership_3)
            create_super_poly(self.config, self.psql_conn, self.shifted_faces, farm_plot_ownership_3, possession_3)
            break_voids_2(self.config, self.psql_conn, farm_plot_ownership_3, farm_plot_ownership_4)
            create_super_poly(self.config, self.psql_conn, self.shifted_faces, farm_plot_ownership_4, possession_4)
            logger.info("cut_farms() executed successfully")
        except:
            logger.error(f"Error in cut_farms()", exc_info=True)
        

    def create_final_possession(self):
        edges_creater = Topology_Edges_Creater(self.config, self.psql_conn)
        edges_creater.run()
        logger.info("Final possession created successfully")
    
    def assigning_farm_vs_void(self):
        try:
            assign_type_farm_vs_void(self.config, self.psql_conn)
            logger.info("assigned types as farm or void")
        except:
            logger.error("could not assign type farm or void", exc_info=True)

    def run(self):
        setup = Setup_Pos(self.config, self.psql_conn)
        setup.run()

        self.assigning_farm_vs_void()
        self.cut_farms()
        self.create_final_possession()
        topo_name = f'{self.village}_{self.farm_topo}'
        polygonize_topo(self.psql_conn, self.village, topo_name, self.possession_final)
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Description for my parser")

    parser.add_argument("-v", "--village", help="Village name",
                        required=False, default="")

    argument = parser.parse_args()

    village = argument.village
    pos = possession(village)
    pos.run()
