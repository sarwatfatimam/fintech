import time
from fintech.etl.master_data import MasterData
from fintech.etl.ochl_data import OCHLData
from fintech.etl.financial_data import FinanceData
from fintech.etl.etl_status import ETLStatus
from fintech.utils.db import SQliteDB
from fintech.utils.logs import info


def etl():

    ETLStatus('master_data').create_db_table()
    info("Processing finance data for FinanceData")
    FinanceData().execute()
    time.sleep(60)
    info("Processing Ticker and Sector for MasterData")
    MasterData().execute()
    time.sleep(60)
    info("Processing OCHL for OCHLData")
    OCHLData().execute()


if __name__ == '__main__':
    etl()
    # Testing that table are created and loaded
    db_master = SQliteDB('master_data')
    db_finance = SQliteDB('finance_data')
    db_ochl = SQliteDB('ochl_data')
    # print('master', db_master.select('SELECT * FROM ETLStatus;'))
    # print('master', db_master.select('SELECT name FROM sqlite_schema WHERE type="table" and name="ETLStatus";'))
    # print('master', db_master.select('SELECT name FROM sqlite_schema WHERE type="table";'))
    # print('finance', db_finance.select('SELECT name FROM sqlite_schema WHERE type="table";'))
    # print('ohcl', db_ochl.select('SELECT name FROM sqlite_schema WHERE type="table";'))
    # print(db_master.select('SELECT DateTime, Remarks FROM ETLStatus'))
    # print(db_finance.select('SELECT Distinct Value, ReportPeriod FROM FinanceData'))
    # print(db_ochl.select('SELECT distinct date FROM TickerOCHLAll'))
    print(db_ochl.select('SELECT * FROM TickerOCHLAll').shape)
    # print(db_finance.select('SELECT * FROM FinanceData').shape)
    # print(db_finance.select('SELECT Distinct Indicator, ReportPeriod FROM FinanceData'))
    print(db_master.select('SELECT * FROM Tickerlist').shape)
