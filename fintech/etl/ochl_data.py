import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta


class OCHLData:

    def __init__(self):
        self.schema = read_meta('fintech', 'tickerochl', 'etl/config/')['TickerOCHL']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/ochl'
        self.db = SQliteDB('ochl_data')

    def create_db_table(self):
        self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def processing(self):
        df_ochl_all = pd.read_csv(self.raw_dir_path+'/ticker_ochl_all.csv')
        df_ochl_all.rename(columns={'Stock Splits': 'StockSplits'}, inplace=True)
        return df_ochl_all

    def execute(self):
        self.create_db_table()
        df = self.processing()
        self.insert_db_table(df)
