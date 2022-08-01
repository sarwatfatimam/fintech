import csv
import os
import re

import dateutil.parser as dparser
import numpy as np
import pandas as pd

from fintech.etl.cdc import CDC
from fintech.etl.etl_status import ETLStatus
from fintech.utils.db import SQliteDB
from fintech.utils.helper import move_processed_file, sorting_files_on_modification_dt
from fintech.utils.helper import read_meta
from fintech.utils.logs import info, exception


class FinanceData:

    def __init__(self):
        self.schema = read_meta('fintech', 'finance', 'etl/config/')['Finance']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.constraints = pd.DataFrame(self.schema['constraints'])
        self.raw_dir_path = './fintech/raw/finance'
        self.process_dir_path = './fintech/raw/processed'
        self.df_data = pd.DataFrame(columns=self.mapping['name'].tolist())
        self._raw_files = []
        self.db = SQliteDB('finance_data')
        self.df_sector = pd.DataFrame()
        self.etlstatus = ETLStatus('finance_data_import')
        self.indicator_keys = ['Country', 'Ticker', 'Year', 'Month', 'Indicator']
        self.cdc = CDC(self.indicator_keys, self.db, self.schema['name'])
        self.na_list = ['nan', np.nan, None, 'None', 'none']

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
            self.db.create_table(mappings=self.mapping, table_name=self.schema['name'],
                                 constraints=self.constraints['cols'][0])
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
                                        temp['ReportPeriod'] = None
                                        temp.rename(columns={c + 1: 'Value', 0: 'Period'}, inplace=True)
                                        index = temp[(temp['Period'].str.contains(',', regex=True)) |
                                                     (temp['Period'].isin(self.na_list))].index.values[0]
                                        temp.loc[0:index - 1, 'ReportPeriod'] = 'Annual'
                                        temp.loc[index + 1:len(temp) + 1, 'ReportPeriod'] = 'Quarter'
                                        indicator_df = pd.concat([indicator_df, temp])
                                        indicator_df[['Country', 'Ticker', 'Sector', 'RawFile',
                                                      'LastUpdatedDateTime']] = country, ticker, sector, file, \
                                                                                update_date
                                processed_dfs.append(indicator_df)
                            line_count += 1
                processed_dfs = pd.concat(processed_dfs)
                processed_dfs['PeriodTemp'] = processed_dfs['Period'].replace(r'[a-zA-Z]|\,|\.|/', '', regex=True)
                processed_dfs['Year'] = processed_dfs['PeriodTemp'].astype(str).str[:4]
                processed_dfs['Month'] = processed_dfs['PeriodTemp'].astype(str).str[4:6]
                temp_mqrtr = (np.ceil(pd.to_numeric(processed_dfs['Month'], downcast='integer') / 3)
                              ).astype(str).replace(r'\.0$', '', regex=True)
                temp_mqrtr = temp_mqrtr.replace({val: '' for val in self.na_list})
                temp_qrtr = temp_mqrtr.apply(lambda x: 'Q' if x not in [''] else '')
                processed_dfs['Quarter'] = temp_qrtr + temp_mqrtr
                update_dt_qrtr = processed_dfs['LastUpdatedDateTime'].apply(lambda x: 'Q' + str(x.quarter))
                processed_dfs['Quarter'] = np.where(processed_dfs['Period'].str.contains(r'[a-zA-Z]|,|/', na=False),
                                                    update_dt_qrtr, processed_dfs['Quarter'])
                processed_dfs['Quarter'] = processed_dfs['Quarter'].str.replace(r'\.0$', '', regex=True)
                df = processed_dfs[['Country', 'Ticker', 'Sector', 'Year', 'Month', 'Quarter', 'Indicator', 'Value',
                                    'ReportPeriod', 'LastUpdatedDateTime', 'RawFile']]
                df = df.drop_duplicates(subset=self.constraints['cols'][0])
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
