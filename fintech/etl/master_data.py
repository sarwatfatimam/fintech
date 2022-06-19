import os
import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta
from fintech.utils.helper import move_processed_file


class MasterData:

    def __init__(self):
        self.schema = read_meta('fintech', 'tickerlist', 'etl/config/')['TickerList']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/master'
        self.process_dir_path = './fintech/raw/processed'
        self.db = SQliteDB('master_data')
        self._sector = pd.DataFrame()

    @property
    def sector(self):
        if self._sector.empty:
            db = SQliteDB('finance_data')
            query = 'SELECT Distinct Country, Ticker, Sector FROM FinanceData'
            self._sector = db.select(query)
            self._sector.rename(columns={'Sector': 'sector_gf',
                                         'Country': 'country',
                                         'Ticker': 'ticker'}, inplace=True)
        return self._sector

    def create_db_table(self):
        self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def processing(self):
        file_list = os.listdir(self.raw_dir_path)
        if len(file_list) != 0:
            df_ticker_list = pd.read_csv(self.raw_dir_path + '/ticker_list_us.csv')
            df_ticker_list.rename(columns={'Symbol': 'Ticker'}, inplace=True)
            df_sector_list = pd.read_csv(self.raw_dir_path + '/ticker_sector_us.csv')
            df_sector_list = pd.concat([df_sector_list, self.sector]).drop_duplicates().reset_index(drop='index')
            df_sector_list.to_csv(self.raw_dir_path + '/ticker_sector_us.csv', mode='w', index=False)
            df_sector_list.rename(columns={'country': 'Country', 'ticker': 'Ticker', 'sector_gf': 'Sector_gf'},
                                  inplace=True)
            df_ticker_list = df_ticker_list.join(df_sector_list.set_index(['Country', 'Ticker']), on=['Country',
                                                                                                      'Ticker'])
            df_ticker_list.rename(columns={'Security Name': 'SecurityName'}, inplace=True)
            move_processed_file(self.raw_dir_path, self.process_dir_path, 'ticker_list_us.csv')
            move_processed_file(self.raw_dir_path, self.process_dir_path, 'ticker_sector_us.csv')
            self.insert_db_table(df_ticker_list)
        else:
            print('No new data to be processed for Master Ticker List and Sector')

    def execute(self):
        self.create_db_table()
        self.processing()
