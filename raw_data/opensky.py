from opensky_api import OpenSkyApi
from utils.helper_functions import *

class OpenApi:
    def __init__(self):
        params = get_parameters()
        if params.get('opensky_username') and params.get('opensky_password'):
            # registered users requests return within 5 seconds
            self.open_sky = OpenSkyApi(
                username=params['opensky_username'], 
                password=params['opensky_password']
                )
        else:
            # returns within 10 seconds
            self.open_sky = OpenSkyApi()

    def get_states(self):
        states = self.open_sky.get_states()
        return states

