from pyspark.sql import SparkSession
import sys
import os

#To initiate Spark session
def CreateSparkSession():
    spark = SparkSession.builder.master("local[1]")\
            .appName("CSV2Parquet")\
            .getOrCreate()

    import pyspark.sql.types as pst
    return spark,pst

def CSV2Parquet(raw_path,stg_path):
    spark,pst = CreateSparkSession()
    CsvSchema = pst.StructType([pst.StructField("vendor_id",pst.StringType(),True),
                                pst.StructField("pickup_datetime",pst.TimestampType(),True),
                                pst.StructField("dropoff_datetime",pst.TimestampType(),True),
                                pst.StructField("passenger_count",pst.IntegerType(),True),
                                pst.StructField("trip_distance",pst.DoubleType(),True),
                                pst.StructField("rate_code_id",pst.IntegerType(),True),
                                pst.StructField("store_and_fwd_flag",pst.StringType(),True),
                                pst.StructField("pickup_location_id",pst.IntegerType(),True),
                                pst.StructField("dropoff_location_id",pst.IntegerType(),True),
                                pst.StructField("payment_type",pst.StringType(),True),
                                pst.StructField("fare_amount",pst.DoubleType(),True),
                                pst.StructField("extra",pst.DoubleType(),True),
                                pst.StructField("mta_tax",pst.DoubleType(),True),
                                pst.StructField("tip_amount",pst.DoubleType(),True),
                                pst.StructField("tolls_amount",pst.DoubleType(),True),
                                pst.StructField("improvement_surcharge",pst.DoubleType(),True),
                                pst.StructField("total_amount",pst.DoubleType(),True),
                                pst.StructField("congestion_surcharge", pst.DoubleType(), True)
                        ])
    #Reads the CSV data from raw folder to DataFrame
    df = spark.read.format("csv").option("header", "true").schema(CsvSchema).load(raw_path)
    #removing row duplicates
    df = df.distinct()
    #writes the data into staging Layer as Parquet format
    df.write.format("parquet").mode("overwrite").save(stg_path)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError('please provide a Valid year between 2015 to 2020.')
    year = sys.argv[1]
    home_path = os.path.expanduser('~')
    raw_path = f"{home_path}/Desktop/NYC_Taxi/raw/yellow_tripdata_{year}*.csv"
    stg_path = f"{home_path}/Desktop/NYC_Taxi/staging/yellow_tripdata"
    CSV2Parquet(raw_path, stg_path)