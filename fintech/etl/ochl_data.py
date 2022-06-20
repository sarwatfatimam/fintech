import os
import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta
from fintech.utils.helper import move_processed_file, sorting_files_on_modification_dt


class OCHLData:

    def __init__(self):
        self.schema = read_meta('fintech', 'tickerochl', 'etl/config/')['TickerOCHL']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/ochl'
        self.process_dir_path = './fintech/raw/processed'
        self.db = SQliteDB('ochl_data')

    def create_db_table(self):
        self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def processing(self):
        file_list = os.listdir(self.raw_dir_path)
        if len(file_list) != 0:
            sorted_files = sorting_files_on_modification_dt(self.raw_dir_path, file_list)
            file_list = sorted_files['files'].to_list()
            for file in file_list:
                df_ochl_all = pd.read_csv(f'{self.raw_dir_path}/{file}')
                df_ochl_all.rename(columns={'Stock Splits': 'StockSplits'}, inplace=True)
                move_processed_file(self.raw_dir_path, self.process_dir_path, file)
                self.insert_db_table(df_ochl_all)
        else:
            print('No new data to be processed for ochl')

    def execute(self):
        self.create_db_table()
        self.processing()
