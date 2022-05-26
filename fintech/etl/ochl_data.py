import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta


class OCHLData:

    def __init__(self):
        pass

    def execute(self):
        db = SQliteDB('ochl_data')
        schema = read_meta('fintech', 'tickerochl', 'etl/config/')['TickerOCHL']
        mapping = pd.DataFrame(schema['fields'])
        db.create_table(mappings=mapping, table_name=schema['name'])
        df_ochl_all = pd.read_csv('./fintech/raw/ochl/ticker_ochl_all.csv')
        df_ochl_all.rename(columns={'Stock Splits': 'StockSplits'}, inplace=True)
        db.insert_into(df_ochl_all, table_name=schema['name'])
