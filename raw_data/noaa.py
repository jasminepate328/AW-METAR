from noaa_sdk import NOAA
from utils.helper_functions import *

class WeatherService:
    def __init__(self):
        self.noaa = NOAA()

    def data_by_coordinates(self, lat, long, type=None):
        try:
            if type not in NOAA_RESPONSE_TYPES.append(None): 
                raise("Invalid response type")
            forecast = self.noaa.get_forecasts(lat, long, type)

           # TODO: format data based on type
            match type:
                case 'forecast':
                    return
                case 'forecastHourly':
                    return
                case 'forecastGridData':
                    return
                case _:
                    return forecast
        except Exception as e:
            logger.error(e)
         
    def data_by_location(self, zipcode, country_code):
        try:
            forecast = self.noaa.get_forecasts(zipcode, country_code)
            return forecast
        except Exception as e:
            logger.error(e)

    def get_observations(self, zipcode, country_code, start=None, end=None):
        try:
            observersations = self.noaa.get_observations(zipcode, country_code, start, end)
            return observersations
        except Exception as e:
            logger.error(e)
