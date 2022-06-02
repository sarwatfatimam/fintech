import csv
import math as m
import os
import re

import dateutil.parser as dparser
import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta


class FinanceData:

    def __init__(self):
        self.schema = read_meta('fintech', 'finance', 'etl/config/')['Finance']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/finance'
        self.df_data = pd.DataFrame(columns=self.mapping['name'].tolist())
        self._raw_files = []
        self.db = SQliteDB('finance_data')

    @property
    def raw_files(self):
        if len(self._raw_files) == 0:
            for (dir_path, dir_names, file_names) in os.walk(self.raw_dir_path):
                self._raw_files.extend(file_names)
        return self._raw_files

    def create_db_table(self):
        self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def process_csv_files(self):
        indicator_df = pd.DataFrame()
        processed_dfs = []
        for f in self.raw_files:
            with open(f'{self.raw_dir_path}/{f}') as csv_file:
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
                                temp['indicator'] = aqd_transposed[c + 1][0]
                                temp.rename(columns={c + 1: 'value', 0: 'Period'}, inplace=True)
                                indicator_df = pd.concat([indicator_df, temp])
                                indicator_df[['Country', 'Ticker', 'Sector', 'RawFile',
                                              'LastUpdatedDateTime', 'ReportPeriod']] = country, ticker, sector, \
                                                                                        f, update_date, None
                                indicator_df['Year'] = indicator_df['Period'].apply(
                                    lambda x: x[0:4]).replace(r'[a-zA-Z]', '', regex=True)
                                indicator_df['Month'] = indicator_df['Period'].apply(
                                    lambda x: x[4:6]).replace(r'[a-zA-Z]', '', regex=True)
                                indicator_df['Quarter'] = indicator_df['Month'].replace("", None).astype(int).apply(
                                    lambda x: 'Q' + str(m.ceil(x / 3)))
                        processed_dfs.append(indicator_df)
                    line_count += 1
        return pd.concat(processed_dfs)

    def execute(self):
        self.create_db_table()
        df = self.process_csv_files()
        self.insert_db_table(df)
