import csv
import os
import re

import dateutil.parser as dparser
import numpy as np
import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.etl.cdc import CDC
from fintech.utils.helper import read_meta
from fintech.etl.etl_status import ETLStatus
from fintech.utils.logs import info, exception
from fintech.utils.helper import move_processed_file, sorting_files_on_modification_dt


class FinanceData:

    def __init__(self):
        self.schema = read_meta('fintech', 'finance', 'etl/config/')['Finance']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/finance'
        self.process_dir_path = './fintech/raw/processed'
        self.df_data = pd.DataFrame(columns=self.mapping['name'].tolist())
        self._raw_files = []
        self.db = SQliteDB('finance_data')
        self.df_sector = pd.DataFrame()
        self.etlstatus = ETLStatus('finance_data_import')
        self.indicator_keys = ['Country', 'Ticker', 'Year', 'Month', 'Indicator']
        self.cdc = CDC(self.indicator_keys, self.db, self.schema['name'])

    @property
    def raw_files(self):
        if len(self._raw_files) == 0:
            for (dir_path, dir_names, file_names) in os.walk(self.raw_dir_path):
                self._raw_files.extend(file_names)
        return self._raw_files

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

    def process_csv_files(self):
        file_list = os.listdir(self.raw_dir_path)
        sorted_files = sorting_files_on_modification_dt(self.raw_dir_path, file_list)
        file_list = sorted_files['files'].to_list()
        df_prev = self.get_prev_data().drop_duplicates()
        self.etlstatus.scheduled(file_list)
        if len(file_list) != 0:
            try:
                indicator_df = pd.DataFrame()
                processed_dfs = []
                for file in self.raw_files:
                    self.etlstatus.start(file)
                    print(f'Processing file {file}')
                    with open(f'{self.raw_dir_path}/{file}') as csv_file:
                        csv_reader = csv.reader(csv_file)
                        line_count = 0
                        for row in csv_reader:
                            if line_count == 0:
                                update_date = dparser.parse(row[0], fuzzy=True)
                            if line_count == 4:
                                ticker = row[0]
                                country = row[2]
                                sector = row[3]
                            if re.search('annual data', ','.join([i.lower() for i in row])):
                                annual_quarter_data = pd.read_csv(csv_file, header=None)
                                aqd_transposed = annual_quarter_data.T
                                for c in aqd_transposed.columns.tolist():
                                    if c + 1 <= (len(aqd_transposed.columns.tolist()) - 1):
                                        temp = pd.DataFrame(aqd_transposed[[0, c + 1]][2:])
                                        temp['Indicator'] = aqd_transposed[c + 1][0]
                                        temp.rename(columns={c + 1: 'Value', 0: 'Period'}, inplace=True)
                                        indicator_df = pd.concat([indicator_df, temp])
                                        indicator_df[['Country', 'Ticker', 'Sector', 'RawFile',
                                                      'LastUpdatedDateTime', 'ReportPeriod']] = country, ticker, sector, \
                                                                                                file, update_date, None
                                processed_dfs.append(indicator_df)
                            line_count += 1
                processed_dfs = pd.concat(processed_dfs)
                processed_dfs['PeriodTemp'] = processed_dfs['Period'].replace(r'[a-zA-Z]|\,|\.|/', '', regex=True)
                processed_dfs['Year'] = processed_dfs['PeriodTemp'].astype(str).str[:4]
                processed_dfs['Month'] = processed_dfs['PeriodTemp'].astype(str).str[4:6]
                processed_dfs['Quarter'] = 'Q' + np.ceil(processed_dfs['Month'].replace("",
                                                                                        None).astype(int) / 3).astype(str)
                processed_dfs['Quarter'] = np.where(processed_dfs['Period'].str.contains(r'[a-zA-Z]|,|/', na=False),
                                                    processed_dfs['Period'], processed_dfs['Quarter'])
                processed_dfs['Quarter'] = processed_dfs['Quarter'].str.replace(r'\.0$', '', regex=True)
                df = processed_dfs[['Country', 'Ticker', 'Sector', 'Year', 'Month', 'Quarter', 'Indicator', 'Value',
                                    'ReportPeriod', 'LastUpdatedDateTime', 'RawFile']]
                for file in self.raw_files:
                    move_processed_file(self.raw_dir_path, self.process_dir_path, file)
                    self.etlstatus.complete(file)
                check = self.cdc.check(df_prev, df)
                if check:
                    self.insert_db_table(df)
            except Exception as error:
                self.etlstatus.error(None, exception(str(error)))
        else:
            info('No new data to be processed for Finance')

    def execute(self):
        self.create_db_table()
        self.process_csv_files()
