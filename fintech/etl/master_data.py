import os
import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.etl.cdc import CDC
from fintech.utils.helper import read_meta
from fintech.etl.etl_status import ETLStatus
from fintech.utils.logs import info, exception
from fintech.utils.helper import move_processed_file


class MasterData:

    def __init__(self):
        self.schema = read_meta('fintech', 'tickerlist', 'etl/config/')['TickerList']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/master'
        self.process_dir_path = './fintech/raw/processed'
        self.db = SQliteDB('master_data')
        self._sector = pd.DataFrame()
        self.etlstatus = ETLStatus('master_data_import')
        self.indicator_keys = ['country', 'ticker']
        self.cdc = CDC(self.indicator_keys, self.db, self.schema['name'])

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
        check = self.db.select(f"SELECT name FROM sqlite_schema WHERE type='table' "
                               f"and name = '{self.schema['name']}';")
        if check.empty:
            self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])
        else:
            info('Table is not created as it already exists in db')

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def get_prev_data(self):
        return self.db.select(f'SELECT Distinct {",".join(self.indicator_keys)} FROM {self.schema["name"]}')

    def processing(self):
        file_list = os.listdir(self.raw_dir_path)
        df_prev = self.get_prev_data().drop_duplicates()
        self.etlstatus.scheduled(file_list)
        if len(file_list) != 0:
            try:
                self.etlstatus.start('ticker_list_us.csv')
                df_ticker_list = pd.read_csv(self.raw_dir_path + '/ticker_list_us.csv')
                df_ticker_list.rename(columns={'Symbol': 'Ticker'}, inplace=True)
                df_ticker_list.columns = map(str.lower, df_ticker_list.columns)
                self.etlstatus.start('ticker_sector_us.csv')
                df_sector_list = pd.read_csv(self.raw_dir_path + '/ticker_sector_us.csv')
                df_sector_list = pd.concat([df_sector_list, self.sector]).drop_duplicates().reset_index(drop='index')
                df_sector_list.to_csv(self.raw_dir_path + '/ticker_sector_us.csv', mode='w', index=False)
                df_sector_list.columns = map(str.lower, df_sector_list.columns)
                df_ticker_list = df_ticker_list.join(df_sector_list.set_index(['country', 'ticker']), on=['country',
                                                                                                          'ticker'])
                df_ticker_list.rename(columns={'security name': 'securityname'}, inplace=True)
                df_ticker_list = df_ticker_list[['ticker', 'securityname', 'exchange', 'country', 'sector_gf']]
                check = self.cdc.check(df_prev, df_ticker_list)
                if check:
                    self.insert_db_table(df_ticker_list)
                move_processed_file(self.raw_dir_path, self.process_dir_path, 'ticker_list_us.csv')
                self.etlstatus.complete('ticker_list_us.csv')
                move_processed_file(self.raw_dir_path, self.process_dir_path, 'ticker_sector_us.csv')
                self.etlstatus.complete('ticker_sector_us.csv')
            except Exception as error:
                self.etlstatus.error('ticker_sector_us.csv/ticker_sector_us.csv', exception(str(error)))
        else:
            info('No new data to be processed for Master Ticker List and Sector')

    def execute(self):
        self.create_db_table()
        self.processing()
