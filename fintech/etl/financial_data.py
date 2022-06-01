import os
import re
import csv
import pandas as pd
import dateutil.parser as dparser

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta


class FinanceData:

    def __init__(self):
        self.schema = read_meta('fintech', 'finance', 'etl/config/')['Finance']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.raw_dir_path = './fintech/raw/finance'
        self.df_data = pd.DataFrame(columns=self.mapping['name'].tolist())
        self._raw_files = []

    @property
    def raw_files(self):
        if len(self._raw_files) == 0:
            for (dir_path, dir_names, file_names) in os.walk(self.raw_dir_path):
                self._raw_files.extend(file_names)
        return self._raw_files

    def create_db_table(self):
        db = SQliteDB('finance_data')
        db.create_table(mappings=self.mapping, table_name=self.schema['name'])

    def process_csv_files(self):
        raw_file = []
        country = []
        ticker = []
        sector = []
        update_date = []
        indicator_df = pd.DataFrame()
        for f in self.raw_files:
            raw_file.append(f)
            with open(f'{self.raw_dir_path}/{f}') as csv_file:
                csv_reader = csv.reader(csv_file)
                line_count = 0
                for row in csv_reader:
                    if line_count == 0:
                        update_date.append(dparser.parse(row[0], fuzzy=True))
                    if line_count == 4:
                        ticker.append(row[0])
                        country.append(row[2])
                        sector.append(row[3])
                    if re.search('annual data', ','.join([i.lower() for i in row])):
                        annual_quarter_data = pd.read_csv(csv_file, header=None)
                        aqd_transposed = annual_quarter_data.T
                        for c in aqd_transposed.columns.tolist():
                            temp = pd.DataFrame(aqd_transposed[c][1:])
                            temp['indicator'] = aqd_transposed[c][0]
                            temp.rename(columns={c: 'value'}, inplace=True)
                            indicator_df = pd.concat([indicator_df, temp])
                    line_count += 1
        df = pd.DataFrame({'Country': country, 'Ticker': ticker,
                           'Sector': sector, 'LastUpdatedDateTime': update_date, 'RawFile': raw_file})
        print(df)

    def execute(self):
        self.create_db_table()
        print(self.raw_files)
        self.process_csv_files()
