__author__ = 'nahla.errakik'

import pandas as pd
from io import BytesIO, StringIO
from zipfile import ZipFile

from urllib.request import urlopen


class Station:
    def __init__(self):
        self.server = r'https://s3.amazonaws.com/tripdata/'

    def get_station_df(self, file_name):
        url = r'{server}{file_name}.csv.zip'.format(server=self.server, file_name=file_name)
        raw_data = urlopen(url)
        zip_file = ZipFile(BytesIO(raw_data.read()))
        csv_file = zip_file.open(file_name + '.csv').read().decode()
        df = pd.read_csv(StringIO(csv_file), sep=',')

        return df

    def get_stations(self):
        """
        Returns the stations information for 2019
        :return: a DataFrame of all the used stations in 2019 without duplicates
        """
        columns = ['id', 'name', 'latitude', 'longitude']
        start_stations = pd.DataFrame()
        end_stations = pd.DataFrame()
        stations = pd.DataFrame()
        for x in range(1, 13):
            log_print = '2019-0' + str(x) if x < 10 else '2019-' + str(x)
            print("loading station data for: " + log_print)
            file_name = '2019{}-citibike-tripdata'.format('0' + str(x) if x < 10 else x)
            df = self.get_station_df(file_name=file_name)

            start_stations = start_stations.append(df[['start station id', 'start station name', 'start station latitude',
                                                       'start station longitude']]).drop_duplicates(subset='start station id')
            end_stations = end_stations.append(df[['end station id', 'end station name', 'end station latitude',
                                                   'end station longitude']].drop_duplicates()).drop_duplicates(subset='end station id')

        start_stations.columns = columns
        end_stations.columns = columns
        stations = stations.append([start_stations, end_stations]).drop_duplicates(subset='id').dropna()
        stations['latitude'] = stations['latitude'].apply(lambda i: float(i))
        stations['longitude'] = stations['longitude'].apply(lambda j: float(j))

        return stations
