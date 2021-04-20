__author__ = 'nahla.errakik'

import json
import requests
import pandas as pd
from http import HTTPStatus
from io import BytesIO, StringIO
from zipfile import ZipFile
from urllib.request import urlopen

from . import db


class Collision(db.Model):
    __tablename__ = 'collisions'

    id = db.Column('collision_id', db.Integer, primary_key=True)
    zip_code = db.Column(db.String)
    crash_date = db.Column(db.String)
    crash_time = db.Column(db.String)
    borough = db.Column(db.String)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    on_street_name = db.Column(db.String)
    cross_street_name = db.Column(db.String)
    off_street_name = db.Column(db.String)
    number_of_cyclist_injured = db.Column(db.Integer)
    number_of_cyclist_killed = db.Column(db.Integer)

    def to_dict(self):
        return dict(collision_id=self.id,
                    zip_code=self.zip_code,
                    crash_date=self.crash_date,
                    crash_time=self.crash_time,
                    borough=self.borough,
                    latitude=self.latitude,
                    longitude=self.longitude,
                    on_street_name=self.on_street_name,
                    cross_street_name=self.cross_street_name,
                    off_street_name=self.off_street_name,
                    number_of_cyclist_injured=self.number_of_cyclist_injured,
                    number_of_cyclist_killed=self.number_of_cyclist_killed)

    def insert(self):
        db.session.add(self)
        db.session.commit()

    @staticmethod
    def get_collisions():
        return db.session.query(Collision).all()

    @staticmethod
    def server():
        return "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

    @staticmethod
    def get_cycles_collisions(borough):
        """
        Returns collisions information for injured killed cycles for a given borough
        :param borough: name of borough (example: Queens, Bronx ...)
        :return: collisions information organized in a DataFrame
        """
        if borough is None:
            raise Exception("borough is mandatory")

        url = "{server}?borough={borough}".format(server=Collision.server(), borough=borough)
        response = requests.get(url)
        if response.status_code != HTTPStatus.OK:
            raise Exception("Error while calling external service")

        result = pd.DataFrame(json.loads(response.content))
        if not result.empty:
            result = result.loc[
                (result["number_of_cyclist_injured"] != '0') | (result["number_of_cyclist_killed"] != '0')]

        return result

    @staticmethod
    def get_borough_list():
        """
        Gets the list of boroughs where the collisions occurred
        :return: list of boroughs
        """
        url = "{server}?$select=distinct borough".format(server=Collision.server())
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
                ['collision_id', 'zip_code', 'crash_date', 'crash_time', 'borough', 'latitude', 'longitude',
                 'on_street_name', 'cross_street_name', 'off_street_name', 'number_of_cyclist_injured',
                 'number_of_cyclist_killed']]
            vehicle_df = pd.DataFrame()
            for index, row in data_df.iterrows():
                if 'vehicle_type_code1' in data_df.columns:
                    if str(row['vehicle_type_code1']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series(
                            [row['collision_id'], row['vehicle_type_code1'], row['contributing_factor_vehicle_1']]),
                            ignore_index=True)
                if 'vehicle_type_code2' in data_df.columns:
                    if str(row['vehicle_type_code2']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series(
                            [row['collision_id'], row['vehicle_type_code2'], row['contributing_factor_vehicle_2']]),
                            ignore_index=True)
                if 'vehicle_type_code3' in data_df.columns:
                    if str(row['vehicle_type_code_3']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series(
                            [row['collision_id'], row['vehicle_type_code_3'], row['contributing_factor_vehicle_3']]),
                            ignore_index=True)
                if 'vehicle_type_code_4' in data_df.columns:
                    if str(row['vehicle_type_code_4']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series(
                            [row['collision_id'], row['vehicle_type_code_4'], row['contributing_factor_vehicle_4']]),
                            ignore_index=True)
                if 'vehicle_type_code_5' in data_df.columns:
                    if str(row['vehicle_type_code_5']) != 'nan':
                        vehicle_df = vehicle_df.append(pd.Series(
                            [row['collision_id'], row['vehicle_type_code_5'], row['contributing_factor_vehicle_5']]),
                            ignore_index=True)

            vehicle_df.columns = ['collision_id', 'type_code', 'contributing_factor']

            collision_df['collision_id'] = collision_df['collision_id'].apply(lambda x: int(x))
            collision_df['latitude'] = collision_df['latitude'].apply(lambda x: float(x))
            collision_df['longitude'] = collision_df['longitude'].apply(lambda x: float(x))
            collision_df['number_of_cyclist_injured'] = collision_df['number_of_cyclist_injured'].apply(lambda x: int(x))
            collision_df['number_of_cyclist_killed'] = collision_df['number_of_cyclist_killed'].apply(lambda x: int(x))

            vehicle_df['collision_id'] = vehicle_df['collision_id'].apply(lambda x: int(x))

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


class Vehicle(db.Model):
    __tablename__ = 'vehicles'

    id = db.Column('vehicle_id', db.Integer, primary_key=True, autoincrement=True)
    collision = db.Column(db.Integer, db.ForeignKey('collisions.collision_id'))
    type_code = db.Column(db.String)
    contributing_factor = db.Column(db.String)

    def __init__(self, collision, type_code, contributing_factor):
        self.collision = collision
        self.type_code = type_code
        self.contributing_factor = contributing_factor

    def to_dict(self):
        return dict(vehicle_id=self.id,
                    collision_id=self.collision,
                    type_code=self.type_code,
                    contributing_factor=self.contributing_factor)

    def insert(self):
        """db.session.add(self)
        db.session.commit()"""
        insert_command = Vehicle.__table__.insert(
            prefixes=['OR IGNORE'],
            values=dict(collision=self.collision, type_code=self.type_code, contributing_factor=self.contributing_factor))
        db.session.execute(insert_command)
        db.session.commit()

    @staticmethod
    def get_vehicles():
        return db.session.query(Vehicle).all()

db.create_all()
db.session.commit()
