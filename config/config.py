# from geo_tol import geo_tols
import json
import os

class Config:
    def __init__(self, gcp_path = ""):
        modules = ["setup","psql","georef","fbfs","pos","val","data","fp"]
        self.setup_details = self.get_details(modules)
        
    def get_details(self, modules):
        details = {}
        for key in modules:
            dir_path = os.path.dirname(__file__)
            setup_file = os.path.join(dir_path,f"{key}.json")
            with open(setup_file,'r') as file:
                data = json.loads(file.read())
                details[key] = data
        return details
        
    def get_config(self):
        return self.setup_details

if __name__=="__main__":
    config = Config()
    print(config.get_config())
    