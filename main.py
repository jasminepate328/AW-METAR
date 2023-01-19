import json

from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame as SDF
from pyspark.sql.functions import explode

from raw_data.noaa import WeatherService
from raw_data.opensky import OpenSky

"""
 TODO: 
    Import data
    Get latitude and longitude from OpenSky, use values to pull weather for each location.
    Flatten JSON
    Convert to parquet and save file to S3 
"""

spark = SparkSession.builder.appName('importData').getOrCreate()

def import_data():
    os = OpenSky.get_states

    serialized_json = json.dumps(os.states, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

    with open('raw_data/opensky.json', 'w') as outfile:
        outfile.write(serialized_json) 
    outfile.close()

    with open('raw_data/noaa.json', 'w') as outfile:
        for i in os.states:
            WeatherService.data_by_coordinates(i.latitude, i.longitude, "forecastGridData")
            
    # read and write all files in the diretory.  
    # recursive_loaded_df = spark.read.format("parquet")\
    #     .option("recursiveFileLookup", "true")\
    #     .load("examples/src/main/resources/dir1")


if __name__ == "__main__":
    import_data()