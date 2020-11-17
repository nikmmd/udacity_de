from pyspark.sql import SparkSession
import botocore
import os
import sys
import configparser
from pyspark.sql.types import *
from pyspark.sql.functions import * 

from helpers import (init_geo_func, genetate_date_dim_df)

def transform_load_vendor_dim(spark):
    spark.sql("""
    INSERT OVERWRITE TABLE vendor_dim VALUES ('1', "Creative Mobile Technologies"), ('2', "VeriFone Inc."), ('-1', "Not Available")
    """)

def transform_crash_dim(spark, raw_crash_dim, geo_meta_loc, is_cache_read):
    
    # register udf
    geo_meta_udf = udf(init_geo_func(geo_meta_loc),StructType(
                    [
                        StructField("location_id", IntegerType(), True),
                        StructField("zone", StringType(), True),
                        StructField("borough", StringType(), True)
                    ])) 
    
    casted_crash_dim = raw_crash_dim\
    .select(
    to_date(col("CRASH DATE"), "dd/MM/yyyy").alias("crash_date"),
    col("LATITUDE").cast(DoubleType()).alias("latitude"),
    col("LONGITUDE").cast(DoubleType()).alias("longitude"),
    col("NUMBER OF PERSONS KILLED").cast(IntegerType()).alias("number_of_persons_killed"),
    col("NUMBER OF PERSONS INJURED").cast(IntegerType()).alias("number_of_persons_injured"),
    col("NUMBER OF PEDESTRIANS INJURED").cast(IntegerType()).alias("number_of_pederstrians_injured"),
    col("NUMBER OF PEDESTRIANS KILLED").cast(IntegerType()).alias("number_of_pederstrians_killed"),
    col("NUMBER OF CYCLIST INJURED").cast(IntegerType()).alias("number_of_cyclist_injured"),
    col("NUMBER OF CYCLIST KILLED").cast(IntegerType()).alias("number_of_cyclist_killed"),
    col("NUMBER OF MOTORIST INJURED").cast(IntegerType()).alias("number_of_motorist_injured"),
    col("NUMBER OF MOTORIST KILLED").cast(IntegerType()).alias("number_of_motorist_killed"),
        
    array_distinct(array(
          upper(col("VEHICLE TYPE CODE 1")),
          upper(col("VEHICLE TYPE CODE 2")),
          upper(col("VEHICLE TYPE CODE 3")),
          upper(col("VEHICLE TYPE CODE 4")),
          upper(col("VEHICLE TYPE CODE 5"))
         )).alias("vechicle_type_list"),
    array_distinct(array(
            upper(col("CONTRIBUTING FACTOR VEHICLE 1")),
            upper(col("CONTRIBUTING FACTOR VEHICLE 2")),
            upper(col("CONTRIBUTING FACTOR VEHICLE 3")),
           upper(col("CONTRIBUTING FACTOR VEHICLE 4")),
           upper(col("CONTRIBUTING FACTOR VEHICLE 5")),
    )).alias("contributing_factor_list")
    ).withColumn(
   "impacted_participant_list",
    array_distinct(array(
        when((col("number_of_pederstrians_injured") > 0 ) | (col("number_of_pederstrians_killed") > 0), "Pedestrian"),
          when((col("number_of_cyclist_injured") > 0 ) | (col("number_of_cyclist_killed") > 0), "Cyclist"),
          when((col("number_of_motorist_injured") > 0 ) | (col("number_of_motorist_killed") > 0), "Motorist")
    ))).filter(col("crash_date").isNotNull() & (col("latitude").isNotNull() | col("longitude").isNotNull()))
    
    tmp_table = "casted_crash_dim_enriched_temp"
    enriched_crash_dim = None    
    if int(is_cache_read) == 0:
        print("Building Geo Cache for future usage. Run with GEO_CACHE=1 cfg to speed up ETL next time")
        # this is so that broken table can be overwritten
        spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
        enriched_crash_dim = casted_crash_dim.withColumn("taxi_zone", geo_meta_udf(col("longitude"), col("latitude")))
        enriched_crash_dim.write.mode("overwrite").saveAsTable(tmp_table)
        enriched_crash_dim = spark.table(tmp_table)
    else:
        print("Using Geo Cache")
#         tbl_list = spark.catalog.listTables("default")
#         if not tmp_table in tbl_list:
#             exit("You must first run an uncached version with GEO_CACHE=0")
#         else:
        enriched_crash_dim = spark.table(tmp_table)
        
    spark.sql(f"""
INSERT OVERWRITE TABLE crash_dim 
SELECT 
       concat_ws("", Split(crash_date, '-'))           AS date_key, 
       taxi_zone.location_id                           AS location_id, 
       Sum(1)                                          AS crash_count,
       Sum(number_of_persons_injured)                  AS total_injury_count, 
       Sum(number_of_persons_killed)                   AS total_kill_count, 
       -- collection all exept empty elements from array
       array_except(Flatten(Collect_set(contributing_factor_list)), array(NULL))  AS contributing_factor_list, 
       array_except(Flatten(Collect_set(impacted_participant_list)), array(NULL)) AS impacted_participant_list, 
       array_except(Flatten(Collect_set(vechicle_type_list)), array(NULL))        AS vechicle_type_list 
FROM   {tmp_table} 
GROUP  BY crash_date, 
          location_id 
""")

def transform_load_trip_fact(spark, raw_trip_dim):
    
    df_taxi_trunc = raw_trip_dim.select(
        col("VendorId").cast(IntegerType()).alias("vendor_key"),
        col("tpep_pickup_datetime").cast(DateType()).alias("trip_date"),
        col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        col("PULocationID").cast(IntegerType()).alias("pickup_location_key"),
        col("DOLocationID").cast(IntegerType()).alias("drop_off_location_key"),
        col("total_amount").cast(DecimalType(18,2)).alias("total_payment_amount")
    ).withColumn("vendor_key", when(col("vendor_key").isNull(), -1).otherwise(col("vendor_key")))
    
    df_taxi_trunc.createOrReplaceTempView("taxi_trip_temp")
    
    spark.sql("""
INSERT OVERWRITE TABLE trip_fact 
    SELECT
        vendor_key,
        pickup_location_key,
        drop_off_location_key,
        concat_ws("", Split(trip_date, '-'))           AS date_key,
        SUM(passenger_count)                           AS passenger_count,
        SUM(total_payment_amount)                      AS total_payment_amount,
        SUM(1)                                         AS trip_count
    FROM taxi_trip_temp
    GROUP BY vendor_key, trip_date, pickup_location_key, drop_off_location_key
""")

def trasnform_load_date_dim(spark):
    date_dim_df = genetate_date_dim_df(spark, "2014-01-01", 10000)
    date_dim_df.createOrReplaceTempView("date_dim_temp")
    
    spark.sql("""
    INSERT OVERWRITE table date_dim 
    SELECT 
        date_as_int as date_key,
        day,
        week,
        month,
        quarter,
        year
    FROM date_dim_temp
    """)

def transform_load_location_dim(spark, raw_location_df):
    raw_location_df.createOrReplaceTempView("location_temp")
    spark.sql("""
    INSERT OVERWRITE TABLE location_dim 
    SELECT locationid AS location_id, 
            zone       AS zone, 
            borough    AS borough 
     FROM   location_temp
    """)
    
def create_spark_session(aws_key, aws_secret):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell'
    spark = SparkSession.builder\
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 pyspark-shell")\
        .config("spark.hadoop.fs.s3a.access.key", aws_key)\
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)\
    .enableHiveSupport().getOrCreate()
    return spark

    
if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    spark = create_spark_session(config.get("AWS", "KEY"), config.get("AWS", "SECRET"))
    spark.sql("""select 1""").show()
    
    # Location dim ETL
    df_taxi_zone = spark.read.option("header",True).csv(config.get("S3", "ZONE_LOOKUP_DATA_LOC"))
    print("Loading Location Dim")
    transform_load_location_dim(spark, df_taxi_zone)
    
    # vendor dim ETL
    print("Loading Vendor Dim")
    transform_load_vendor_dim(spark)
    
    # date dim ETL
    print("Loading Date Dim")
    trasnform_load_date_dim(spark)
    
    # crash dim ETL
    df_crash_raw = spark.read.option("header",True).csv(config.get("S3", "CRASH_DATA_LOC"))
    print("Loading Crash Dim")
    transform_crash_dim(spark, df_crash_raw, config.get("S3", "GEO_LOOKUP_DATA_LOC"), config.get("ETL", "GEO_CACHE") )
    
    # Trip fact ETL
    df_taxi = spark.read.option("header",True).csv(config.get("S3", "NYC_TAXI_DATA_LOC"))
    
    print("Loading Trip Fact")
    transform_load_trip_fact(spark, df_taxi)


    
    


