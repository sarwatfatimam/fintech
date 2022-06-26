import pandas as pd
from fintech.utils.logs import info


class CDC:

    def __init__(self, indicator_keys, db, table_name):
        self.indicator_keys = indicator_keys
        self.db = db
        self.table_name = table_name

    def check(self, prev_data, cur_data):
        if prev_data.empty:
            info('New data is available')
        else:
            check_data = pd.concat([prev_data, cur_data[self.indicator_keys]]).drop_duplicates()
            if len(prev_data) == len(check_data):
                info('No new data is available')
                return False
            else:
                info('Updated data is available')
                df = cur_data[self.indicator_keys].drop_duplicates()
                query_params = []
                for c in self.indicator_keys:
                    query_params.append(f"{c} in ({','.join(df[c].drop_duplicates().tolist())})")
                info(f'Dropping previous data from {self.table_name}')
                self.db.execute(f"DELETE FROM {self.table_name} where ' and '.join(query_params)")
                return True
