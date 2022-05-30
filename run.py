from fintech.etl.master_data import MasterData
from fintech.etl.ochl_data import OCHLData
from fintech.etl.financial_data import FinanceData


def etl():

    # MasterData().execute()
    # OCHLData().execute()
    FinanceData().execute()


if __name__ == '__main__':
    etl()
    # Testing that table are created and loaded
    # from fintech.utils.db import SQliteDB
    # db = SQliteDB('master_data')
    # print(db.select('SELECT * FROM TickerList'))
    # db = SQliteDB('ochl_data')
    # print(db.select('SELECT * FROM TickerOCHLAll'))
