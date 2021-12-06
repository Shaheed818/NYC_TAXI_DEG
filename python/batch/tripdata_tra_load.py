from pyspark.sql import SparkSession
import sys
import os

#to initiate spark sessions
def CreateSparkSession():
    spark = SparkSession.builder.master("local[1]")\
            .appName("CSV2Parquet")\
            .getOrCreate()

    import pyspark.sql.types as pst
    import pyspark.sql.functions as psf
    return spark,pst,psf

#Below method does actual loading of data from Staging layer to load layer
def Tra_Load_TripData(stg_path,year):
    spark,pst,psf = CreateSparkSession()
    df = spark.read.format("parquet").load(stg_path)
    df1 = df.withColumn('year',psf.year('dropOff_dateTime'))\
            .withColumn('quarter',psf.quarter('dropOff_dateTime'))\
            .withColumn('hour',psf.hour('dropOff_dateTime'))\
            .withColumn('speed',psf.round(psf.col('trip_distance') * 3600 / (psf.unix_timestamp('dropOff_dateTime') - psf.unix_timestamp('pickup_datetime'))).cast('integer'))
    # Above speed column derived based on calcuation.
    #Creating temp View
    df1.createOrReplaceTempView(f'''v_stg_yellow_tripData''')

    # Loading the tip percentage data per quarter and dropOff_Location_id Level into DataFrame
    df_tip_percentage = spark.sql(f'''select quarter,
                           dropOff_location_id,
                           CAST(sum(tip_amount)/sum(total_amount)*100 AS INT) as tip_percentage
                           from v_stg_yellow_tripData 
                           WHERE year = {year} and tip_amount > 0 
                           and vendor_id is not null 
                           group by quarter,dropOff_location_id
                           ''')

    # loading the max_speed info at trip_date,hour grouping level.
    df_day_speed_stats = spark.sql(f'''select cast(dropOff_dateTime as Date) as Trip_Date
                                            , hour 
                                            , max(speed) as maxSpeed
                                            from v_stg_yellow_tripData 
                                            WHERE year = {year} and vendor_Id is not null 
                                            and trip_Distance > 0
                                            group by Trip_Date,hour order by maxSpeed desc
                                    ''')
    #Need to hardcode the password in place of XXXXX
    prop = {'user': 'root', 'password': 'XXXXX', 'driver': "com.mysql.jdbc.Driver"}
    jdbc_url = f"jdbc:mysql://localhost:3306/test"

    #Loading the df data into using MySQL database
    # JDBC driver is important here to establish the JDBC connection
    df_tip_percentage.write.jdbc(jdbc_url, 'DOL_tip_Quarterly_Agg_Dtls', 'overwrite', prop)
    df_day_speed_stats.write.jdbc(jdbc_url, 'Speed_Day_Hour_Agg_Dtls', 'overwrite', prop)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError('please provide a Valid year between 2015 to 2020.')
    year = sys.argv[1]
    home_path = os.path.expanduser('~')
    stg_path = f"{home_path}/Desktop/NYC_Taxi/staging/yellow_tripdata"
    Tra_Load_TripData(stg_path,year)

