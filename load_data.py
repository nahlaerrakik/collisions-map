__author__ = 'nahla.errakik'

import json
import sqlite3

import pandas as pd
from scripts.collision import Collision
from scripts.station import Station
from utils.sqlite import SQLite

my_col = Collision()
my_sql = SQLite()
my_stat = Station()

collision_creation_query = 'CREATE TABLE "Collision" (' \
                           '"collision_id"	INTEGER,' \
                           '"zip_code"	TEXT,' \
                           '"crash_date"	TEXT,' \
                           '"crash_time"	TEXT,' \
                           '"borough"	TEXT,' \
                           '"latitude"	TEXT,' \
                           '"longitude"	TEXT,' \
                           '"location"	TEXT,' \
                           '"on_street_name"	TEXT ,' \
                           '"cross_street_name"	TEXT,' \
                           '"off_street_name"	TEXT,' \
                           '"number_of_cyclist_injured"	INTEGER,' \
                           '"number_of_cyclist_killed"	INTEGER,' \
                           'PRIMARY KEY("collision_id"));'

vehicle_creation_query = 'CREATE TABLE "Vehicle" (' \
                         '"index"	INTEGER,' \
                         '"collision_id"	INTEGER,' \
                         '"type_code" TEXT,' \
                         '"contributing_factor" TEXT,' \
                         'FOREIGN KEY("collision_id") REFERENCES "Collision"("collision_id"),' \
                         'PRIMARY KEY("index" AUTOINCREMENT));'

station_creation_query = 'CREATE TABLE "Station" (' \
                         '"id"	INTEGER,' \
                         '"name"	TEXT,' \
                         '"latitude" REAL,' \
                         '"longitude" REAL,' \
                         'PRIMARY KEY("id"));'

my_sql.create_table('Collision', collision_creation_query)
my_sql.create_table('Vehicle', vehicle_creation_query)
my_sql.create_table('Station', station_creation_query)

is_inserted = False
boroughs = my_col.get_borough_list()
all_vehicles = pd.DataFrame()
for borough in boroughs:
    data_df = my_col.get_cycles_collisions(borough)
    collisions, vehicles = Collision.process_data(data_df)

    collisions['collision_id'] = collisions['collision_id'].apply(lambda x: int(x))
    collisions['crash_date'] = collisions['crash_date'].apply(lambda x: pd.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f'))
    collisions['crash_time'] = collisions['crash_time'].apply(lambda x: pd.datetime.strptime(x, '%H:%M').time())
    collisions['location'] = collisions['location'].apply(lambda x: json.dumps(x))
    collisions['number_of_cyclist_injured'] = collisions['number_of_cyclist_injured'].apply(lambda x: int(x))
    collisions['number_of_cyclist_killed'] = collisions['number_of_cyclist_killed'].apply(lambda x: int(x))
    try:
        my_sql.insert_data(table='Collision', data=collisions)
        print("loading and inserting data for borough: " + borough)
    except sqlite3.IntegrityError:
        print('Skipping data insertion. Data is already inserted')
        is_inserted = True
        break

    vehicles['collision_id'] = vehicles['collision_id'].apply(lambda x: int(x))
    all_vehicles = all_vehicles.append(vehicles).drop_duplicates()

if not is_inserted:
    my_sql.insert_data(table='Vehicle', data=all_vehicles)

    stations = my_stat.get_stations()
    my_sql.insert_data(table='Station', data=stations)
