from fintech.etl.master_data import MasterData
from fintech.etl.ochl_data import OCHLData
from fintech.etl.financial_data import FinanceData


def etl():

    FinanceData().execute()
    MasterData().execute()
    OCHLData().execute()


if __name__ == '__main__':
    etl()
    # Testing that table are created and loaded
    from fintech.utils.db import SQliteDB
    db = SQliteDB('master_data')
    print('master', db.execute('SHOW TABLES'))
    db = SQliteDB('finance_data')
    print('finance', db.execute('SHOW TABLES'))
    db = SQliteDB('ohcl_data')
    print('ohcl', db.execute('SHOW TABLES'))
    # print(db.select('SELECT * FROM TickerList'))
    # print(db.select('SELECT name FROM sqlite_schema WHERE type="table" ORDER BY name; '))
    # db = SQliteDB('ochl_data')
    # print(db.select('SELECT name FROM sqlite_schema WHERE type="table" ORDER BY name; '))
    # # print(db.select('SELECT * FROM TickerOCHLAll'))
    # db = SQliteDB('finance_data')
    # print(db.select('SELECT name FROM sqlite_schema WHERE type="table" ORDER BY name; '))
