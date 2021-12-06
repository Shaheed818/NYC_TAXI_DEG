# NYC_TAXI_DEG
Data engineering for NYC yellow taxi trip data for different use cases.

Data engineering process to perfrom ETL steps to acheive below use cases on only Yellow Taxi data.

o Identify in which DropOff Location the tip is highest, in proportion to the cost of the ride, per 
quarter 

o Identify at which hour of the day the speed of the taxis is highest (speed = distance/trip 
duration)

Source data: https://www1.nyc.gov/site/tlc/about/raw-data.page

![image](https://user-images.githubusercontent.com/95505625/144856674-3ea2e25e-0a87-4f6d-a247-8ca562425aa7.png)

**Technologies used** 

- Spark
- Python
- MySQL

****Software requirements to run on Local ****

- Python 3.9.6
- Spark 3.2.0
- MYSQL 
- JDBC driver from Maven - mysql-connector-java-8.0.22.jar
- PySpark
- Pycharm(optional)

**ETL Steps: **

Extration - tripdata_imp_csv.py 
It uses Requests library to pull the yellow taxi transactional data for a complete year(2020) into raw Layer(local) as csv format.

Tranform - tripdata_stg_par.py
PySpark job reads the data from raw Layer , then wil load the data as parquest format using defined schema and after removing the duplicates. Parquet files will be created into a local staging Layer.

Load - tripdata_tra_load.py
PySpark job reads the data from staging layer(Parquet files), Then as per the usecases mentioned above it calculates/perfrorms the speed analysis and Tip analysis data. Finally it creates the tables in MySQL and loads the data into below Tables.

Speed_Day_Hour_Agg_Dtls

DOL_tip_Quarterly_Agg_Dtls








