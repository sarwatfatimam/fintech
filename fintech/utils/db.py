import logging
import sqlite3
import pandas as pd

from urllib.request import pathname2url
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import create_engine


class SQliteDB:

    def __init__(self, db_path=None):
        self.db_path = db_path

    def create_connection(self):

        """
        create a database connection to sqlite that resides in the memory or at a specific path with a specific db
        """

        try:
            if self.db_path is None:
                conn = sqlite3.connect(':memory:')
            else:
                try:
                    db_uri = 'file:{}?mode=rw'.format(pathname2url(self.db_path))
                    conn = sqlite3.connect(db_uri, uri=True)
                except Exception as e:
                    conn = sqlite3.connect(self.db_path)

        except Exception as error:
            logging.exception(str(error))
            raise

        else:
            return conn

    def select(self, query, as_dataframe=True):

        conn = None
        try:
            conn = self.create_connection()

            if as_dataframe:
                df = pd.read_sql(query, conn)
                return df
            else:
                result = conn.execute(query)
                result = result.fetchall()
                return result

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            if conn is not None:
                conn.close()

    def drop_table(self, table_name):

        if not isinstance(table_name, str):
            raise TypeError('tablename should be a string')

        if len(table_name) == 0:
            raise ValueError('tablename is empty')

        conn = None

        try:

            conn = self.create_connection()
            conn.execute(f'DROP TABLE IF EXISTS {table_name}')

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            if conn is not None:
                conn.close()

    def insert_into(self, dataframe, table_name, dtypes={}, chunk_size=100000):

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('input must be a dataframe')
        if not isinstance(dtypes, dict):
            raise TypeError('dtypes must be a dictionary')
        if not isinstance(table_name, str):
            raise TypeError('table_name must be a string')
        if not isinstance(chunk_size, int):
            raise TypeError('chunk_size must be an integer')

        try:
            engine = self.create_connection()
            dataframe = dataframe.where((pd.notnull(dataframe)), None)
            dataframe.to_sql(table_name, con=engine, chunksize=chunk_size,
                             if_exists='append', index=False, dtype=dtypes)

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            engine.close()

    def execute(self, query):

        conn = None

        try:
            conn = self.create_connection()
            conn.execute(query)

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            if conn is not None:
                conn.close()

    def create_table(self, mappings, table_name, constraints=[], field_name_column='name', data_type_column='dtype'):

        # Validate Parameters

        if not isinstance(mappings, pd.DataFrame):
            raise TypeError('df_mapping must be a dataframe')

        if not isinstance(table_name, str):
            raise TypeError('table_name must be a str')

        if field_name_column not in list(mappings.columns):
            raise ValueError('df_mapping must have ' + field_name_column + ' column')

        if data_type_column not in list(mappings.columns):
            raise ValueError('df_mapping must have ' + data_type_column + ' column')

        try:
            mappings.drop_duplicates(field_name_column, inplace=True)
            mappings['definition'] = \
                '`' + mappings[field_name_column] + '`' + ' ' + mappings[data_type_column]

            self.drop_table(table_name)
            col_definition = ','.join(mappings['definition'].dropna().values)
            if len(constraints) != 0:
                constraints_col = ','.join(constraints)
                create_query = f'CREATE TABLE {table_name} ({col_definition}, PRIMARY KEY ({constraints_col}))'
            else:
                create_query = f'CREATE TABLE {table_name} ({col_definition})'
            self.execute(create_query)

        except Exception as error:
            logging.exception(str(error))


class DB:

    """
    This class uses SQLAlchemy to create connection and executes queries based on the database driver.
    """

    def __init__(self, host=None, user=None, password=None, db=None, port=None, driver=None):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.driver = driver
        self.config = dict(drivername=self.driver, username=self.user, password=self.password, host=self.host,
                           port=self.port, database=self.db, query={})

    def get_engine(self):

        """
        Given the database configurations, a database engine is returned.
        """

        try:
            db_url = URL(**self.config)
            engine = create_engine(db_url, pool_pre_ping=True, connect_args={'connect_timeout': 10000})

        except Exception as error:
            logging.exception(str(error))
            raise

        else:
            return engine

    def select(self, query, as_dataframe=True):

        conn = None
        try:
            engine = self.get_engine()
            conn = engine.connect()

            if as_dataframe:
                df = pd.read_sql(query, conn)
                return df
            else:
                result = conn.execute(query)
                result = result.fetchall()
                return result

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            if conn is not None:
                conn.close()
                engine.dispose()

    def drop_table(self, table_name):

        if not isinstance(table_name, str):
            raise TypeError('tablename should be a string')

        if len(table_name) == 0:
            raise ValueError('tablename is empty')

        conn = None

        try:

            engine = self.get_engine()
            conn = engine.connect()

            conn.execute(f'DROP TABLE IF EXISTS {table_name}')

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            if conn is not None:
                conn.close()
                engine.dispose()

    def insert_into(self, dataframe, table_name, dtypes={}, chunk_size=10000):

        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError('input must be a dataframe')
        if not isinstance(dtypes, dict):
            raise TypeError('dtypes must be a dictionary')
        if not isinstance(table_name, str):
            raise TypeError('table_name must be a string')
        if not isinstance(chunk_size, int):
            raise TypeError('chunk_size must be an integer')

        try:
            engine = self.get_engine()
            dataframe = dataframe.where((pd.notnull(dataframe)), None)
            dataframe.to_sql(table_name.lower(), con=engine, chunksize=chunk_size,
                             if_exists='append', index=False, dtype=dtypes)

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            engine.dispose()

    def execute(self, query):

        conn = None

        try:
            engine = self.get_engine()
            conn = engine.connect()
            conn.execute(query)

        except Exception as error:
            logging.exception(str(error))
            raise

        finally:
            if conn is not None:
                conn.close()
                engine.dispose()

    def create_table(self, mappings, table_name, field_name_column='name', data_type_column='dtype'):

        # Validate Parameters

        if not isinstance(mappings, pd.DataFrame):
            raise TypeError('df_mapping must be a dataframe')

        if not isinstance(table_name, str):
            raise TypeError('table_name must be a str')

        if field_name_column not in list(mappings.columns):
            raise ValueError('df_mapping must have ' + field_name_column + ' column')

        if data_type_column not in list(mappings.columns):
            raise ValueError('df_mapping must have ' + data_type_column + ' column')

        try:
            mappings.drop_duplicates(field_name_column, inplace=True)
            mappings['definition'] = \
                '`' + mappings[field_name_column] + '`' + ' ' + mappings[data_type_column] + ' ' + 'DEFAULT NULL'

            self.drop_table(table_name)
            col_definition = ','.join(mappings['definition'].dropna().values)
            create_query = f'CREATE TABLE {table_name} ({col_definition}) ENGINE=MyISAM DEFAULT CHARSET=latin1;'
            self.execute(create_query)

        except Exception as error:
            logging.exception(str(error))
