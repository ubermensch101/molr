from geo_tol import geo_tols

class Config:
    def __init__(self, gcp_path = ""):
        self.setup_details = self.get_details()
        if gcp_path != "":
            self.setup_details["gcp_path"] = gcp_path
    
    def get_details(self):
        cur_dict = {}
        cur_dict["geo_tols"] = geo_tols
        return cur_dict

    def get_config(self):
        return self.setup_details

if __name__=="__main__":
    config = Config()
    print(config.get_config())
    