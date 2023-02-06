import json
from opensky_api import OpenSkyApi
from utils.helper_functions import *

class OpenSky:
    def __init__(self):
        params = get_parameters(['opensky_username', 'opensky_password'])
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
        os_states = self.open_sky.get_states()
        return json.dumps(os_states.states, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

