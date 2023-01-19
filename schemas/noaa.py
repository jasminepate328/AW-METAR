from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampNTZType, MapType

forecast_schema = StructType(fields=[
    # Unit: degC, Duration: every 3 hours
    StructField('temperature', MapType(StringType(), StructType([
        StructField('uom', StringType(), False), # Determines the unit of measurement
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: degC, Duration: once a day
    StructField('maxTemperature', MapType(StringType(), StructType([
        StructField('uom', StringType(), False),
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: degC, Duration: once a day
    StructField('minTemperature', MapType(StringType(), StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: degC, Duration: every hour
    StructField('windChill', MapType(StringType(), StructType([
        StructField('uom', StringType(), False),
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: Percent, Duration: every hour
    StructField('skyCover', MapType(StringType(), StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: degree(angle), Duration: sporadically 
    StructField('windDirection', MapType(StringType(), StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: km_h-1, Duration: sporadically 
    StructField('windSpeed', MapType(StringType(), StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))),
    # Unit: km_h-1, Duration: sporadically 
    StructField('windGust', MapType(StringType(), StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampNTZType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])))
])
