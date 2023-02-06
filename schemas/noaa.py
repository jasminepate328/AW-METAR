from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType, MapType

forecast_schema = StructType(fields=[
    # Unit: degC, Duration: every 3 hours
    StructField('temperature', StructType([
        StructField('uom', StringType(), False), # Determines the unit of measurement
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: degC, Duration: once a day
    StructField('maxTemperature', StructType([
        StructField('uom', StringType(), False),
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: degC, Duration: once a day
    StructField('minTemperature', StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: degC, Duration: every hour
    StructField('windChill', StructType([
        StructField('uom', StringType(), False),
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: Percent, Duration: every hour
    StructField('skyCover', StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: degree(angle), Duration: sporadically 
    StructField('windDirection', StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: km_h-1, Duration: sporadically 
    StructField('windSpeed', StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ])),
    # Unit: km_h-1, Duration: sporadically 
    StructField('windGust', StructType([
        StructField('uom', StringType(), False), 
        StructField(
        'values', ArrayType(
            StructType([
                StructField('validTime', TimestampType(), False),
                StructField('value', DoubleType(), False)
                ])
            )
        )
    ]))
])
