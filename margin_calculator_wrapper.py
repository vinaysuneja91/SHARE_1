from margin_calculator import get_margin_data
from output_module import write_to_mysql,write_to_csv
from input_module import read_from_mysql
from input_module import exe_in_mysql
from gsheets import write_to_gsheet
import pandas as pd
from option_chain import get_option_chain_data
from nse_tools_api import fno_lot_sizes, ltp_stock

from datetime import datetime
from pytz import timezone
format = "%Y-%m-%d %H:%M:%S %Z%z"
# Current time in UTC
now_utc = datetime.now(timezone('UTC'))
now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))

import sys

# call function
print("Starting FnO Lot Size function")
fno_lot_size = fno_lot_sizes()
write_to_csv(fno_lot_size, 'fno_lot_size')
write_to_mysql(fno_lot_size, 'fno_lot_size', 'replace')
print('deleting indexes')
exe_in_mysql('delete from fno_lot_size where STOCK_SYMBOL in (\'NIFTY\', \'BANKNIFTY\', \'FINNIFTY\')')
# write_to_gsheet
print('Writing to gsheet')
fno_stocks_lot_prices = read_from_mysql("select d.LIQUID_SNO,STOCK_SYMBOL,LOT_SIZE, LTP \
                                          , LOT_CURRENT_PRICE,INSERT_TS \
                                          from v_fno_stocks_lots_prices t \
                          inner join mystocks.v_options_oi_live_data d on t.STOCK_SYMBOL = d.SCRIP")
fno_stocks_lot_prices_df = pd.DataFrame(fno_stocks_lot_prices
                                         , columns=['LIQUID_SNO','STOCK_SYMBOL', 'LOT_SIZE'
                                                    , 'LTP','LOT_CURRENT_PRICE','INSERT_TS'])
write_to_gsheet(fno_stocks_lot_prices_df, 'gstocks-api', 0)

print("Starting LTP function")
ltp_stock = ltp_stock('FnO')
# output to csv
write_to_csv(ltp_stock, 'ltp_stock')
# output to mysql
write_to_mysql(ltp_stock, 'ltp_stock', 'replace')
# write_to_gsheet
print('Writing to gsheet')
write_to_gsheet(ltp_stock, 'gstocks-api', 1)

print('Getting Option Chain Data')
#stocklist = ['NIFTY']
fno_tuple = read_from_mysql("select STOCK_SYMBOL from fno_lot_size")
#print(type(fno_tuple))
fno_list = list(sum(fno_tuple, ()))
#fno_list = fno_list[:1]
#fno_list = ['RELIANCE']
df_ce_data, df_pe_data, df_oi_data = get_option_chain_data(fno_list)

write_to_mysql(df_ce_data, 't_options_ce_live_data', 'replace')
write_to_mysql(df_pe_data, 't_options_pe_live_data', 'replace')
write_to_mysql(df_oi_data, 't_options_oi_live_data', 'replace')

print('Option Chain Data Completed ')

def margin_calculator_wrapper():
    # Calling for CE Strikes
    print('Getting Margin data for the Strike Prices ')
    option_key_data = read_from_mysql("select * from V_OPTION_KEY_CE")
    #    "select * from V_OPTION_KEY_CE where STRIKE_PRICE = '33000' and expiryDate = '2021-05-06'")

    margin_data_df = get_margin_data(option_key_data)
    print(margin_data_df)
    # print(type(margin_data_df))
    # print(margin_data_df.empty)

    if margin_data_df.empty:
        print('Nothing')
        exe_in_mysql("truncate table t_margin_amt_ce;")
    else:
        # write to mysql
        write_to_mysql(margin_data_df, 't_margin_amt_ce', 'replace')
        print("Write Succesful for CE")
 
    # Calling for PE Strikes
    option_key_data = read_from_mysql("select * from V_OPTION_KEY_PE")
    #    "select * from V_OPTION_KEY_PE where STRIKE_PRICE='33000' and expiryDate='2021-05-06'")
    margin_data_df = get_margin_data(option_key_data)

    if margin_data_df.empty:
        print('Nothing')
        exe_in_mysql("truncate table t_margin_amt_pe;")

    else:
        # write to mysql
        write_to_mysql(margin_data_df, 't_margin_amt_pe', 'replace')
        print("Write Succesful for PE")

    # combining tables

    print(exe_in_mysql("truncate table t_margin_amt;"))
    print(exe_in_mysql("insert into t_margin_amt select * from t_margin_amt_ce union all select * from t_margin_amt_pe;"))

    print('Reading from Db for CE')
    l1 = read_from_mysql('select * from V_OPTIONS_CC_LIST_FILTERED')
    # l1_col = read_from_mysql(
    #    '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='mystocks' AND TABLE_NAME='V_OPTIONS_CC_LIST_FILTERED';''')
    # print(l1_col)

    df = pd.DataFrame(l1, columns=['EXP_MONTH', 'EXP_DATE', 'underlying', 'strikePrice', 'underlyingValue', 'lastPrice', 'PCT_ROI', 'LOT_SIZE',
                                    'LIQUID_SNO', 'PCT_ABOVE', 'TOTAL_MARGIN_AMT', 'TOTAL_REQUIRED_AMT', 'TOTAL_PREMIUM_AMT',
                                    'HIGHEST_OI_STRIKE_PRICE','HIGHEST_OI_OPEN_INTEREST'])
    print(df)

    print(type(df))

    # write_to_gsheet
    print('Writing to gsheet')
    write_to_gsheet(df, 'gstocks-api', 2)

    l2 = read_from_mysql('select * from V_OPTIONS_CSP_LIST_FILTERED')
    # l1_col = read_from_mysql(
    #    '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='mystocks' AND TABLE_NAME='V_OPTIONS_CC_LIST_FILTERED';''')
    # print(l1_col)

    df2 = pd.DataFrame(l2, columns=['EXP_MONTH', 'EXP_DATE', 'underlying', 'strikePrice', 'underlyingValue', 'lastPrice', 'PCT_ROI', 'LOT_SIZE',
                                    'LIQUID_SNO', 'PCT_ABOVE', 'TOTAL_MARGIN_AMT', 'TOTAL_REQUIRED_AMT', 'TOTAL_PREMIUM_AMT',
                                    'HIGHEST_OI_STRIKE_PRICE','HIGHEST_OI_OPEN_INTEREST'])
    print(df2)

    print(type(df2))

    # write_to_gsheet
    print('Writing to gsheet')
    write_to_gsheet(df2, 'gstocks-api', 3)

if len(sys.argv) > 1:
    print('Getting Margin Data')
    if  sys.argv[1] == "M":
        margin_calculator_wrapper()

#l3 = [now_asia]
#l3 = read_from_mysql("select DATE_FORMAT(DATE_ADD(UTC_TIMESTAMP(), INTERVAL 330 MINUTE), '%Y-%m-%d %h:%i:%s %p')")

l3 = read_from_mysql("select DATE_ADD(UTC_TIMESTAMP(), INTERVAL 330 MINUTE)")
df3 = pd.DataFrame(l3, columns=['LAST_RUN_TS'])
print(df3)
# write_to_gsheet
print('Writing to gsheet')
write_to_gsheet(df3, 'gstocks-api', 4)
