__author__ = 'nahla.errakik'

from scripts.collision import Collision
from scripts.station import Station
from utils.sqlite import SQLite
from utils.map import Map

my_col = Collision()
my_sql = SQLite()
my_stat = Station()


def main():
    my_map = Map(starting_latitude=40.72005157518289, starting_longitude=-74.05800137942732)
    collision_dataset = my_sql.get_data("SELECT c.*, GROUP_CONCAT(v.type_code) as 'vehicles', GROUP_CONCAT(v.contributing_factor) as 'contributing_factors' "
                                        "FROM  Collision as c, Vehicle as v "
                                        "WHERE c.collision_id=v.collision_id "
                                        "AND c.location != 'NaN' "
                                        "GROUP BY c.collision_id")
    collision_dataset = collision_dataset.groupby(['borough'])
    for name, df in collision_dataset:
        feature_info = Collision.format_collision_map_data(df)
        my_map.add_feature_group(feature_info=feature_info, name=name, color='lightblue')

    station_dataset = my_sql.get_data("SELECT * FROM Station")
    for index, row in station_dataset.iterrows():
        my_map.add_marker(location=[float(row['latitude']), float(row['longitude'])], popup=row['name'], color='green')

    my_map.render_map()
