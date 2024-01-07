import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class DataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = None

    def read_csv(self):
        self.data = pd.read_csv(self.file_path, encoding='UTF-8')

    def convert_to_datetime(self, column):
        self.data[column] = pd.to_datetime(self.data[column])

    def sort_by_date(self, column):
        self.data = self.data.sort_values(column)


class PostgresConnector:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def connect(self):
        connection_params = {
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port
        }
        self.conn = psycopg2.connect(**connection_params)

    def create_schema(self, schema_name):
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self.conn.commit()
        cursor.close()

    def create_table(self, schema_name, table_name, table_columns):
        cursor = self.conn.cursor()
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                {table_columns}
            )
        """
        cursor.execute(create_table_query)
        self.conn.commit()
        cursor.close()

    def load_data(self, schema_name, table_name, dataframe):
        engine = create_engine(f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}")
        dataframe.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)

    def close_connection(self):
        self.conn.close()

