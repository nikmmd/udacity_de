from pyspark.sql.types import *
from pyspark.sql.functions import *
from shapely.geometry import shape, GeometryCollection, Point
import json


def init_geo_func(geo_json_loc):
    '''
    geo_taxi_loc: geo json file loc

    returns UDF 
    '''
    f = open(geo_json_loc, 'r')
    js = json.load(f)

    def get_geo_meta(x, y):
        if x is None or y is None:
            return None
        point = Point(x, y)

        for feature in js['features']:

            polygon = shape(feature['geometry'])

            if polygon.contains(point):
                props = feature["properties"]
                return dict(
                    location_id=int(props["location_id"]),
                    zone=props["zone"],
                    borough=props["borough"]
                )
        return None

    return get_geo_meta


def genetate_date_dim_df(spark, start_date, size):
    '''
    start_date: Spark parsable date to generate the sequence from like dd-mm-yyyy

    size: Number of days into the future to generate

    returns: date_dim df

    '''
    return spark.range(size).select(
        col("id")
        # pyspark functions throw error, have to use expr and string builder
    ).withColumn("date", expr(f"date_add('{start_date}', id)"))\
     .withColumn("day", dayofmonth(col("date")))\
     .withColumn("week", weekofyear(col("date")))\
     .withColumn("month", month(col("date")))\
     .withColumn("quarter", quarter(col("date")))\
     .withColumn("year", year(col("date")))\
     .withColumn("date_as_int", concat_ws("", split(col("date"), "-")).cast(IntegerType()))
