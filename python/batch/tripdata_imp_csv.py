import requests
import sys
import os

#This programme will pull the files from NYC TLC website for only a year(2020) and Yellow taxi transactional data.
def imp_TripData (year, color):
    home_path = os.path.expanduser('~')
    session = requests.Session()
    for month in range(4,7):
        url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{color}_tripdata_{year}-{str(month).zfill(2)}.csv"
        print(url)
        #url =  'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-06.csv'
        raw_fileName = f"{home_path}/Desktop/NYC_Taxi/raw/{color}_tripdata_{year}_{str(month).zfill(2)}.csv"
        r = session.get(url,stream=True)
        print(f'r done')
        with open(raw_fileName, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)
                print(f'w done')
if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise ValueError('please provide a Valid year between 2015 to 2020.')
    year = sys.argv[1]
    #This will pull only yellow transactionData
    imp_TripData(year,'yellow')


