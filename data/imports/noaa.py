import json
from datetime import datetime, timedelta
from noaa_sdk import NOAA
from utils.helper_functions import *

class WeatherService:
    def __init__(self, start=datetime.today() - timedelta(days=1), end=datetime.today()):
        self.noaa = NOAA()
        self.start = start
        self.end = end

    def data_by_coordinates(self, lat, long, type=None):
        try:
            if type not in NOAA_RESPONSE_TYPES: 
                raise("Invalid response type")
            forecast_output = self.noaa.points_forecast(lat, long, type)
            match type:
                case 'forecast': # returns data in 12 hour intervels
                    return
                case 'forecastHourly':
                    return
                case 'forecastGridData': # 1 hour intervels grouped by data category type
                    return json.dumps(forecast_output['properties'], default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
                case _:
                    return forecast_output
        except Exception as e:
            logging.error(e)
         
    def data_by_location(self, zipcode, country_code):
        try:
            forecast = self.noaa.get_forecasts(zipcode, country_code)
            return forecast
        except Exception as e:
            logging.error(e)

    def get_observations(self, zipcode, country_code):
        try:
            observersations = self.noaa.get_observations(zipcode, country_code, self.start, self.end)
            return observersations
        except Exception as e:
            logging.error(e)
