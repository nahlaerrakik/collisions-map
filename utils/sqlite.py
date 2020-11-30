__author__ = 'nahla.errakik'

import os
import sqlite3
import pandas as pd
from pathlib import Path


class SQLite:
    def __init__(self):
        self.database = 'mvc_collisions.sqlite'
        parent_path = Path(os.path.abspath(os.path.dirname(__file__))).parent
        self.dir_name = os.path.join(parent_path, 'data')

    def create_table(self, table_name, query):
        """
        Creates table in the database
        :param table_name: name of the table
        :param query: SQL query of creation
        :return: None
        """
        with sqlite3.connect(os.path.join(self.dir_name, self.database)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{name}'".format(name=table_name))
            if cursor.fetchone()[0] == 1:
                print('Skipping creation of table in database for: {}. Table already exist'.format(table_name))
            else:
                cursor.execute(query)

    def insert_data(self, table, data):
        """
        Insert data into a table in a database
        :param table: name of the table
        :param data: data to insert
        :return: None
        """
        with sqlite3.connect(os.path.join(self.dir_name, self.database)) as conn:
            data.to_sql(table, conn, if_exists='append', index=False)

    def get_data(self, query):
        """
        Gets data from a database using and SQL query
        :param query: SQL query
        :return: result of the query
        """
        with sqlite3.connect(os.path.join(self.dir_name, self.database)) as conn:
            data_df = pd.read_sql(query, con=conn)
            return data_df
