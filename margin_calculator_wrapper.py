from margin_calculator import get_margin_data
from output_module import write_to_mysql
from input_module import read_from_mysql
from input_module import exe_in_mysql
from gsheets import write_to_gsheet
import pandas as pd
from option_chain import get_option_chain_data


print('Getting Option Chain Data')
#stocklist = ['NIFTY']
fno_tuple = read_from_mysql("select STOCK_SYMBOL from fno_lot_size")
#print(type(fno_tuple))
fno_list = list(sum(fno_tuple, ()))
fno_list = ['ADANIENT']
get_option_chain_data(fno_list)
print('Option Chain Data Completed ')

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
                               'LOT_PRICE', 'LIQUID_SNO', 'PCT_ABOVE', 'TOTAL_MARGIN_AMT', 'TOTAL_REQUIRED_AMT', 'TOTAL_PREMIUM_AMT '])
print(df)

print(type(df))

# write_to_gsheet
print('Writing to gsheet')
write_to_gsheet(df, 'gstocks-api', 0)

l2 = read_from_mysql('select * from V_OPTIONS_CSP_LIST_FILTERED')
# l1_col = read_from_mysql(
#    '''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='mystocks' AND TABLE_NAME='V_OPTIONS_CC_LIST_FILTERED';''')
# print(l1_col)

df2 = pd.DataFrame(l2, columns=['EXP_MONTH', 'EXP_DATE', 'underlying', 'strikePrice', 'underlyingValue', 'lastPrice', 'PCT_ROI', 'LOT_SIZE',
                                'LOT_PRICE', 'LIQUID_SNO', 'PCT_ABOVE', 'TOTAL_MARGIN_AMT', 'TOTAL_REQUIRED_AMT', 'TOTAL_PREMIUM_AMT '])
print(df2)

print(type(df2))

# write_to_gsheet
print('Writing to gsheet')
write_to_gsheet(df2, 'gstocks-api', 1)
