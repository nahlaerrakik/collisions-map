__author__ = 'nahla.errakik'

import pandas as pd
import requests
import json
from http import HTTPStatus


class Collision:
    def __init__(self):
        self.server = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

    def get_cycles_collisions(self, borough):
        """
        Returns collisions information for injured killed cycles for a given borough
        :param borough: name of borough (example: Queens, Bronx ...)
        :return: collisions information organized in a DataFrame
        """
        if borough is None:
            raise Exception("borough is mandatory")

        url = "{server}?borough={borough}".format(server=self.server, borough=borough)
        response = requests.get(url)
        if response.status_code != HTTPStatus.OK:
            raise Exception("Error while calling external service")

        result = pd.DataFrame(json.loads(response.content))
        if not result.empty:
            result = result.loc[
                (result["number_of_cyclist_injured"] != '0') | (result["number_of_cyclist_killed"] != '0')]

        return result

    def get_borough_list(self):
        """
        Gets the list of boroughs where the collisions occurred
        :return: list of boroughs
        """
        url = "{server}?$select=distinct borough".format(server=self.server)
        response = requests.get(url)
        if response.status_code != HTTPStatus.OK:
            raise Exception("Error while calling external service")

        response = json.loads(response.content)
        boroughs = [x['borough'] for x in response if 'borough' in x]
        return boroughs

    @staticmethod
    def process_data(data_df):
        """
        Processes the data returned by the API and format it into different data models
        :param data_df: collisions dataset
        :return: Collisions and Vehicle DataFrame
        """
        if not data_df.empty:
            collision_df = data_df[
                ['collision_id', 'zip_code', 'crash_date', 'crash_time', 'borough', 'latitude', 'longitude', 'location',
                 'on_street_name', 'cross_street_name', 'off_street_name', 'number_of_cyclist_injured',
                 'number_of_cyclist_killed']]
            vehicle_df = pd.DataFrame()
            for index, row in data_df.iterrows():
                if 'vehicle_type_code1' in data_df.columns:
                    if str(row['vehicle_type_code1']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series([row['collision_id'], row['vehicle_type_code1'], row['contributing_factor_vehicle_1']]), ignore_index=True)
                if 'vehicle_type_code2' in data_df.columns:
                    if str(row['vehicle_type_code2']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series([row['collision_id'], row['vehicle_type_code2'], row['contributing_factor_vehicle_2']]), ignore_index=True)
                if 'vehicle_type_code3' in data_df.columns:
                    if str(row['vehicle_type_code_3']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series([row['collision_id'], row['vehicle_type_code_3'], row['contributing_factor_vehicle_3']]), ignore_index=True)
                if 'vehicle_type_code_4' in data_df.columns:
                    if str(row['vehicle_type_code_4']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series([row['collision_id'], row['vehicle_type_code_4'], row['contributing_factor_vehicle_4']]), ignore_index=True)
                if 'vehicle_type_code_5' in data_df.columns:
                    if str(row['vehicle_type_code_5']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series([row['collision_id'], row['vehicle_type_code_5'], row['contributing_factor_vehicle_5']]), ignore_index=True)

            vehicle_df.columns = ['collision_id', 'type_code', 'contributing_factor']

            return collision_df, vehicle_df

        empty_df = pd.DataFrame()
        return empty_df, empty_df

    @staticmethod
    def format_collision_map_data(data_df):
        """
        Format the collision information in order to display in a map
        :param data_df: collisions dataset
        :return: formatted list of collision
        """
        result = []
        for index, row in data_df.iterrows():
            item = dict(location=[row['latitude'], row['longitude']],
                        popup='Borough: {borough}<br>'
                              'Zip Code: {zip_code}<br>'
                              'Crash Date: {crash_date}<br>'
                              'Crash Time: {crash_time}<br>'
                              'Borough: {borough}<br>'
                              'Nbr of injured cyclist: {number_of_cyclist_injured} <br>'
                              'Nbr of killed cyclist: {number_of_cyclist_killed}<br>'
                              'Vehicles: {vehicles}'
                        .format(borough=row['borough'],
                                zip_code=row['zip_code'],
                                crash_date=row['crash_date'],
                                crash_time=row['crash_time'],
                                number_of_cyclist_injured=row['number_of_cyclist_injured'],
                                number_of_cyclist_killed=row['number_of_cyclist_killed'],
                                contributing_factors=row['contributing_factors'],
                                vehicles=row['vehicles']))

            result.append(item)

        return result
