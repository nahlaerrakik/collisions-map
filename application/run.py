__author__ = 'nahla.errakik'

import pandas as pd
from flask import render_template
from sqlalchemy.exc import IntegrityError

from application.models import Collision, Vehicle
from application.utils import Map

from application import app, db


@app.route('/', methods=['GET'])
def home():
    boroughs = Collision.get_borough_list()
    for borough in boroughs:
        data_df = Collision.get_cycles_collisions(borough)
        collisions_dt, vehicles_dt = Collision.process_data(data_df)

        collisions_list = Collision.get_collisions()
        if len(collisions_list) > 0:
            collisions_db = pd.DataFrame.from_records([c.to_dict() for c in collisions_list])
            collisions = collisions_dt.merge(collisions_db, how='outer', indicator=True).loc[
                lambda x: x['_merge'] == 'left_only']
        else:
            collisions = collisions_dt

        vehicles_list = Vehicle.get_vehicles()
        if len(vehicles_list) > 0:
            vehicles_db = pd.DataFrame.from_records([v.to_dict() for v in vehicles_list])
            vehicles = vehicles_dt.merge(vehicles_db, how='outer', indicator=True).loc[
                lambda x: x['_merge'] == 'left_only']
        else:
            vehicles = vehicles_dt

        for index, row in collisions.iterrows():
            new_collision = Collision(id=row['collision_id'],
                                      zip_code=row['zip_code'],
                                      crash_date=row['crash_date'],
                                      crash_time=row['crash_time'],
                                      borough=row['borough'],
                                      latitude=row['latitude'],
                                      longitude=row['longitude'],
                                      on_street_name=row['on_street_name'],
                                      cross_street_name=row['cross_street_name'],
                                      off_street_name=row['off_street_name'],
                                      number_of_cyclist_injured=row['number_of_cyclist_injured'],
                                      number_of_cyclist_killed=row['number_of_cyclist_killed'])
            try:
                new_collision.insert()
            except IntegrityError:
                db.session.rollback()
                print('Skipping collision data insertion. Data is already inserted')
                continue
            except Exception as e:
                db.session.rollback()
                print(e)

        for index, row in vehicles.iterrows():
            new_vehicle = Vehicle(collision=row['collision_id'],
                                  type_code=row['type_code'],
                                  contributing_factor=row['contributing_factor'])
            try:
                new_vehicle.insert()
            except IntegrityError:
                print('Skipping vehicle data insertion. Data is already inserted')
                continue

    my_map = Map(starting_latitude=40.72005157518289, starting_longitude=-74.05800137942732)
    collision_dataset = db.session.query(Collision,
                                         db.func.group_concat(Vehicle.type_code),
                                         db.func.group_concat(Vehicle.contributing_factor)).\
        join(Vehicle, Collision.id == Vehicle.collision).filter(Collision.longitude != 0.0, Collision.latitude != 0.0).\
        group_by(Collision.id).group_by(Collision.borough).all()

    merge_dict = lambda a, b: a.update(b) or a
    map_collision = pd.DataFrame.from_records(
        [merge_dict(c[0].to_dict(), {'vehicles': c[1], 'contributing_factors': c[2]})
        for c in collision_dataset]).groupby(['borough'])

    for name, df in map_collision:
        feature_info = Collision.format_collision_map_data(df)
        my_map.add_feature_group(feature_info=feature_info, name=name, color='lightblue')
    my_map.render_map()

    return render_template('index.html')


if __name__ == "__main__":
    app.run(debug=False)
