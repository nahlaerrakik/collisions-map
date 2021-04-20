__author__ = 'nahla.errakik'

import os
import folium

from pathlib import Path
from folium import FeatureGroup, LayerControl


class Map:
    def __init__(self, starting_latitude, starting_longitude):
        self.map = folium.Map(location=[starting_latitude, starting_longitude], tiles='Open Street Map', zoom_start=10,
                              width='90%', height='90%')
        parent_path = Path(os.path.abspath(os.path.dirname(__file__)))
        self.dir_name = os.path.join(os.path.join(parent_path, 'templates'), 'index.html')

    def add_marker(self, location, popup, color):
        """
        Adds a market in the map
        :param location: list of latitude and longitude
        :param popup: content of the marker popup
        :param color: color of the marker
        :return: None
        """
        popup = folium.Popup(popup, max_width=100, min_width=100)
        icon = folium.Icon(color=color)
        folium.Marker(location=location, popup=popup, icon=icon).add_to(self.map)

    def add_feature_group(self, feature_info, name, color):
        """
        Creates a group of markers and adds it to the map
        :param feature_info: list of markers to add in the feature
        :param name: name of the feature
        :param color: color to apply to the markers of the feature
        :return: None
        """
        feature_group = FeatureGroup(name=name)
        for item in feature_info:
            popup = folium.Popup(item['popup'], max_width=300, min_width=300)
            folium.Marker(location=item['location'], popup=popup, tooltip=name, icon=folium.Icon(color=color)).add_to(feature_group)

        feature_group.add_to(self.map)

    def render_map(self):
        """
        Renders the map object into an html file
        :return: None
        """
        LayerControl().add_to(self.map)
        self.map.save(self.dir_name)
