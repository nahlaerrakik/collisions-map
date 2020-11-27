__author__ = 'nahla.errakik'

import os
import pandas as pd
from pathlib import Path


class Station:
    def __init__(self):
        parent_path = Path(os.path.abspath(os.path.dirname(__file__))).parent
        data_path = os.path.join(parent_path, 'data')
        self.dir_base = os.path.join(data_path, 'citibikes')

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
            print("loading station data for: 2019-" + '0' + str(x) if x < 10 else x)
            file_name = '2019{}-citibike-tripdata.csv'.format('0' + str(x) if x < 10 else x)
            df = pd.read_csv(os.path.join(self.dir_base, file_name), low_memory=False)

            start_stations = start_stations.append(df[['start station id', 'start station name', 'start station latitude', 'start station longitude']]).drop_duplicates(subset='start station id')
            end_stations = end_stations.append(df[['end station id', 'end station name', 'end station latitude', 'end station longitude']].drop_duplicates()).drop_duplicates(subset='end station id')

        start_stations.columns = columns
        end_stations.columns = columns
        stations = stations.append([start_stations, end_stations]).drop_duplicates(subset='id').dropna()
        stations['latitude'] = stations['latitude'].apply(lambda i: float(i))
        stations['longitude'] = stations['longitude'].apply(lambda j: float(j))

        return stations
