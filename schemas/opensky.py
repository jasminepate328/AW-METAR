from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampNTZType, MapType, BooleanType

states_schema = StructType(fields=[
    StructField('latitude', DoubleType(), False),
    StructField('longitude', DoubleType(), False),
    StructField('on_ground', BooleanType(), False),
    StructField('origin_country', StringType(), False),
    StructField('time_position', IntegerType(), False),
    StructField('velocity', DoubleType(), False),
    StructField('baro_altitude', DoubleType(), False),
    StructField('icao24', StringType(), False)
])