from sapextractor.database_connection.interface import DatabaseConnection
from sapextractor.utils.string_matching import find_corr
import sqlite3
import pandas as pd
from sapextractor.utils import constants


class SqliteConnection(DatabaseConnection):
    def __init__(self, path):
        self.path = path
        self.con = sqlite3.connect(self.path)
        self.TIMESTAMP_FORMAT = "%d.%m.%Y %H:%M:%S"
        self.DATE_FORMAT_INTERNAL = "%Y%m%d"
        self.HOUR_FORMAT_INTERNAL = "%H%M%S"
        self.DATE_FORMAT = None
        constants.TIMESTAMP_FORMAT = self.TIMESTAMP_FORMAT
        constants.DATE_FORMAT_INTERNAL = self.DATE_FORMAT_INTERNAL
        constants.HOUR_FORMAT_INTERNAL = self.HOUR_FORMAT_INTERNAL
        constants.DATE_FORMAT = self.DATE_FORMAT
        self.table_prefix = ""
        DatabaseConnection.__init__(self)

    def execute_read_sql(self, sql, columns):
        cursor = self.con.cursor()
        cursor.execute(sql)
        stream = []
        df = []
        while True:
            res = cursor.fetchmany(10000)
            if len(res) == 0:
                break
            for row in res:
                el = {}
                for idx, col in enumerate(columns):
                    el[col] = row[idx]
                stream.append(el)
            this_dataframe = pd.DataFrame(stream)
            df.append(this_dataframe)
            stream = None
            stream = []
        if df:
            df = pd.concat(df)
        else:
            df = pd.DataFrame({x: [] for x in columns})
        df.columns = [x.upper() for x in df.columns]
        return df

    def get_list_tables(self):
        cursor = self.con.cursor()
        cursor.execute('SELECT name from sqlite_master where type= "table"')
        tables = cursor.fetchall()
        tables = [x[0] for x in tables]
        return tables

    def write_dataframe(self, dataframe, table_name):
        dataframe.to_sql(table_name, con=self.con)

    def get_columns(self, table_name):
        cursor = self.con.cursor()
        cursor.execute("SELECT name FROM PRAGMA_TABLE_INFO('%s')" % table_name)
        columns = cursor.fetchall()
        columns = [x[0] for x in columns]
        return columns

    def format_table_name(self, table_name):
        return table_name

    def prepare_query(self, table_name, columns):
        table_name = self.format_table_name(table_name)
        table_columns = self.get_columns(table_name)
        columns = find_corr.apply(columns, table_columns)
        return "SELECT "+",".join(columns)+" FROM "+table_name

    def prepare_and_execute_query(self, table_name, columns, additional_query_part="", return_query=False):
        query = self.prepare_query(table_name, columns) + additional_query_part
        dataframe = self.execute_read_sql(query, columns)
        dataframe.columns = columns
        if return_query:
            return dataframe, query
        return dataframe


def apply(path='./sap.sqlite'):
    return SqliteConnection(path)


def cli():
    print("\n\n")
    print("== Connection to a SQLite database == \n\n")
    path = input("Insert the path to the SQLite database (default: ./sap.sqlite):")
    if not path:
        path = "./sap.sqlite"
    con = apply(path)
    return con
