import pandas as pd

from fintech.utils.db import SQliteDB
from fintech.utils.helper import read_meta


class MasterData:

    def __init__(self):
        pass

    def execute(self):
        db = SQliteDB('master_data')
        schema = read_meta('fintech', 'tickerlist', 'etl/config/')['TickerList']
        mapping = pd.DataFrame(schema['fields'])
        db.create_table(mappings=mapping, table_name=schema['name'])
        df_ticker_list = pd.read_csv('./fintech/raw/master/ticker_list_us.csv')
        df_sector_list = pd.read_csv('./fintech/raw/master/ticker_sector_us.csv').set_index(['country', 'ticker'])
        df_ticker_list = df_ticker_list.join(df_sector_list, on=['Country', 'Symbol'])
        df_ticker_list.rename(columns={'Security Name': 'SecurityName', 'sector_gf': 'Sector_gf'}, inplace=True)
        db.insert_into(df_ticker_list, table_name=schema['name'])
