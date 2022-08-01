import os
import pandas as pd

from fintech.etl.etl_status import ETLStatus
from fintech.etl.cdc import CDC
from fintech.utils.db import SQliteDB
from fintech.utils.logs import info, exception
from fintech.utils.helper import read_meta
from fintech.utils.helper import move_processed_file, sorting_files_on_modification_dt


class OCHLData:

    def __init__(self):
        self.schema = read_meta('fintech', 'tickerochl', 'etl/config/')['TickerOCHL']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.constraints = pd.DataFrame(self.schema['constraints'])
        self.raw_dir_path = './fintech/raw/ochl'
        self.process_dir_path = './fintech/raw/processed'
        self.db = SQliteDB('ochl_data')
        self.etlstatus = ETLStatus('ochl_import')
        self.indicator_keys = ['country', 'ticker', 'date']
        self.cdc = CDC(self.indicator_keys, self.db, self.schema['name'])

    def create_db_table(self):
        check = self.db.select(f"SELECT name FROM sqlite_schema WHERE type='table' "
                               f"and name = '{self.schema['name']}';")
        if check.empty:
            self.db.create_table(mappings=self.mapping, table_name=self.schema['name'],
                                 constraints=self.constraints['cols'][0])
        else:
            info('Table is not created as it already exists in db')

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def get_prev_data(self):
        return self.db.select(f'SELECT Distinct {",".join(self.indicator_keys)} FROM {self.schema["name"]}')

    def processing(self):
        file_list = os.listdir(self.raw_dir_path)
        df_prev = self.get_prev_data().drop_duplicates()
        if len(file_list) != 0:
            sorted_files = sorting_files_on_modification_dt(self.raw_dir_path, file_list)
            file_list = sorted_files['files'].to_list()
            self.etlstatus.scheduled(file_list)
            for file in file_list:
                try:
                    self.etlstatus.start(file)
                    df_ochl_all = pd.read_csv(f'{self.raw_dir_path}/{file}')
                    df_ochl_all.rename(columns={'Stock Splits': 'stocksplits'}, inplace=True)
                    df_ochl_all.columns = map(str.lower, df_ochl_all.columns)
                    df_ochl_all['date'] = df_ochl_all['date'].astype('datetime64[ns]').dt.date
                    df_ochl_all = df_ochl_all.drop_duplicates(subset=self.constraints['cols'][0])
                    check = self.cdc.check(df_prev, df_ochl_all)
                    if check:
                        self.insert_db_table(df_ochl_all)
                    move_processed_file(self.raw_dir_path, self.process_dir_path, file)
                    self.etlstatus.complete(file)
                except Exception as error:
                    self.etlstatus.error(file, exception(str(error)))
        else:
            info('No new data to be processed for ochl')

    def execute(self):
        self.create_db_table()
        self.processing()
