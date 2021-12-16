## this is good for getting historical data ##
from nsepy import *
from datetime import date, timedelta
from output_module import write_to_mysql
import pandas as pd
from input_module import read_from_mysql
from nse_tools_api import get_stock_symbol

if date.today().weekday() == 5:
    run_date = date.today() - timedelta(days=1)

if date.today().weekday() == 6:
    run_date = date.today() - timedelta(days=2)

print("Running for-", run_date)

expiry_date = read_from_mysql(
    'select max(expiry_date) as exp_date from expiry_dates')
# print(type(expiry_date))
curr_exp_date = expiry_date[0]
print('Expiry Date -', curr_exp_date[0])

fno_stocks_list = read_from_mysql(
    "select STOCK_SYMBOL from v_stocks_detail where FNO_FLAG='Y'")
print(fno_stocks_list)
print(type(fno_stocks_list))


def get_future_data():
    #    nse_list = get_stock_symbol()
    #    nse_list = ['INFY', 'SBIN']
    #    print(nse_list)
    fut_df = pd.DataFrame()

    for symbol in fno_stocks_list:
        print('Running for ', symbol)
        stock_fut = get_history(  # symbol="SBIN",
            symbol=symbol,
            start=run_date,
            end=run_date,
            #start=date(2021, 4, 9),
            #end=date(2021, 4, 9),
            futures=True,
            #expiry_date=date(2021, 4, 29)
            expiry_date=curr_exp_date[0]
        )
#        print(stock_fut)
        fut_df = fut_df.append(stock_fut)
#        print(fut_df)
    return fut_df


fut_df = get_future_data()
print(fut_df)

# SBIN APR FUT
write_to_mysql(fut_df, 't_future_data', 'replace')
