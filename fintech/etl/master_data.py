import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta


class MasterData:

    def __init__(self):
        self.schema = read_meta('fintech', 'tickerlist', 'etl/config/')['TickerList']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/master'
        self.db = SQliteDB('master_data')

    def create_db_table(self):
        self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def processing(self):
        df_ticker_list = pd.read_csv(self.raw_dir_path + '/ticker_list_us.csv')
        df_sector_list = pd.read_csv(self.raw_dir_path + '/ticker_sector_us.csv').set_index(['country', 'ticker'])
        df_ticker_list = df_ticker_list.join(df_sector_list, on=['Country', 'Symbol'])
        df_ticker_list.rename(columns={'Security Name': 'SecurityName', 'sector_gf': 'Sector_gf',
                                       'Symbol': 'Ticker'}, inplace=True)
        return df_ticker_list

    def execute(self):
        self.create_db_table()
        df = self.processing()
        self.insert_db_table(df)
