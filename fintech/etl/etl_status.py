import datetime

import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta
from fintech.utils.logs import info


class ETLStatus:

    def __init__(self, function):
        self.schema = read_meta('fintech', 'etl_status', 'etl/config/')['ETLStatus']
        self.mapping = pd.DataFrame(self.schema['fields'])
        self.db = SQliteDB('master_data')
        self.function = function

    def create_db_table(self):
        check = self.db.select(f"SELECT name FROM sqlite_schema WHERE type='table' "
                               f"and name = '{self.schema['name']}';")
        if check.empty:
            self.db.create_table(mappings=self.mapping, table_name=self.schema['name'])
        else:
            info('Table is not created as it already exists in db')

    def insert_db_table(self, df):
        self.db.insert_into(df, table_name=self.schema['name'])

    def set_log_data(self, file, progress, remarks=None):
        return pd.DataFrame({'Function': [self.function],
                             'File': [file],
                             'Progress': [progress],
                             'DateTime': [datetime.datetime.now()],
                             'Remarks': [remarks]})

    def scheduled(self, file_list):
        for file in file_list:
            df = self.set_log_data(file, 'Scheduled')
            self.insert_db_table(df)

    def start(self, file):
        df = self.set_log_data(file, 'Started')
        self.insert_db_table(df)

    def complete(self, file):
        df = self.set_log_data(file, 'Completed')
        self.insert_db_table(df)

    def error(self, file, remarks):
        df = self.set_log_data(file, 'Error', remarks)
        self.insert_db_table(df)
