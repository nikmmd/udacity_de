def create_model(spark):
    spark.sql(
        """
    DROP TABLE IF EXISTS trip_fact
    """
    )

    spark.sql("""
    DROP TABLE IF EXISTS date_dim
    """)

    spark.sql("""
    DROP TABLE IF EXISTS vendor_dim
    """)

    spark.sql("""
    DROP TABLE IF EXISTS location_dim
    """)

    spark.sql("""
    DROP TABLE IF EXISTS crash_dim
    """)

    spark.sql(
        """

    CREATE TABLE trip_fact (
        vendor_key int,
        pickup_location_key int,
        drop_off_location_key int,
        date_key int,
        passenger_count int,
        total_payment_amount decimal(18, 2),
        trip_count int
        )STORED AS PARQUET 
        """
    )

    spark.sql("""
    CREATE TABLE date_dim(
        date_key int,
        day tinyint,
        week tinyint,
        month tinyint,
        quarter tinyint,
        year int
    ) STORED AS PARQUET
    """)

    spark.sql("""
    CREATE TABLE vendor_dim(
        vendor_key tinyint,
        name string
    ) STORED AS PARQUET
    """)

    spark.sql("""
    CREATE TABLE location_dim(
        location_key int,
        zone string,
        borough string
    ) STORED AS PARQUET
    """)

    spark.sql("""
    CREATE TABLE crash_dim(
        date_key int,
        location_key int,
        crash_count bigint,
        person_injury_count bigint,
        person_kill_count bigint,
        contributing_factor_list ARRAY<STRING>,
        impacted_participant_list ARRAY<STRING>,
        vehicle_type_list  ARRAY<STRING>
    ) STORED as PARQUET
    """)
