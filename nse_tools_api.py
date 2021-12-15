from nsetools import Nse
from pprint import pprint
import pandas as pd
from output_module import write_to_csv, write_to_mysql
from datetime import date
from date_time_module import now_asia
import math
from input_module import read_from_mysql
from gsheets import write_to_gsheet

from datetime import datetime
from pytz import timezone
format = "%Y-%m-%d %H:%M:%S %Z%z"
# Current time in UTC
now_utc = datetime.now(timezone('UTC'))
now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))

# initializing
nse = Nse()


def get_stock_symbol():
    all_stock_codes = nse.get_stock_codes()
    # below stmt gives all the keys of dict
    stock_symbol = [*all_stock_codes]
    stock_symbol.pop(0)
    return stock_symbol


def get_top_gainers_losers():
    top_gainers = pd.DataFrame(nse.get_top_gainers())
    top_losers = pd.DataFrame(nse.get_top_losers())
    top_gainers['INSERT_DT'] = now_asia
    top_losers['INSERT_DT'] = now_asia
    return top_gainers, top_losers


def fno_lot_sizes():
    nse = Nse()
    fno_lot_size = pd.DataFrame(
        nse.get_fno_lot_sizes().items(), columns=['STOCK_SYMBOL', 'LOT_SIZE'])
    fno_lot_size['INSERT_TS'] = now_asia
    return fno_lot_size


def ltp_stock(LtpStcoks):
    nse = Nse()
    if LtpStcoks == 'All':
        # for all stocks
        stock_symbol = get_stock_symbol()
    elif LtpStcoks == 'FnO':
        # for only fno stocks
        query_result = read_from_mysql(        # "select STOCK_SYMBOL,LTP,150000 from v_stocks_detail where FNO_FLAG='Y'")
        "select STOCK_SYMBOL from FNO_LOT_SIZE where STOCK_SYMBOL not in ('FINNIFTY','NIFTY', 'BANKNIFTY')")

        query_list = list(sum(query_result, ()))
        stock_symbol = query_list
  # stock_symbol = stock_symbol[:2]
    elif LtpStcoks == 'Master':
        # for only fno stocks
        query_result = read_from_mysql(        # "select STOCK_SYMBOL,LTP,150000 from v_stocks_detail where FNO_FLAG='Y'")
        "select STOCK_SYMBOL from T_STOCK_SYMBOL_MASTER where STOCK_SYMBOL not in ('FINNIFTY','NIFTY', 'BANKNIFTY')")

        query_list = list(sum(query_result, ()))
        stock_symbol = query_list


    temp_dict = {}
    for symbol in stock_symbol:
        print(symbol)
        if nse.is_valid_code(symbol):
            try:
                q = nse.get_quote(symbol)
                averagePrice = (q.get('averagePrice', 0))
            except:
                averagePrice = 0
        else:
            averagePrice = 0
        temp_dict.update({symbol: averagePrice})
        # print(temp_dict)
        # averagePrice
        # print(temp_dict)
    ltp_stock = pd.DataFrame(
        temp_dict.items(), columns=['STOCK_SYMBOL', 'LTP'])

    index_list = ['NIFTY 50', 'NIFTY BANK']  # , 'NIFTY FIN SERVICE']
    index_temp_dict = {}

    for index_symbol in index_list:
        if index_symbol == 'NIFTY 50':
            index_symbol_1 = 'NIFTY'
        if index_symbol == 'NIFTY BANK':
            index_symbol_1 = 'BANKNIFTY'
#        if index_symbol == 'NIFTY FIN SERVICE':
#            index_symbol_1 = 'NIFTY'
        q = nse.get_index_quote(index_symbol)
        # print(q)
        averagePrice = (q.get('lastPrice', 0))
        index_temp_dict.update({index_symbol_1: averagePrice})

#    print(index_temp_dict)
    index_temp_df = pd.DataFrame(
        index_temp_dict.items(), columns=['STOCK_SYMBOL', 'LTP'])

#    ltp_stock = index_temp_df
    ltp_stock = ltp_stock.append(index_temp_df)

    ltp_stock[['LTP']] = ltp_stock[['LTP']].apply(pd.to_numeric)
    ltp_stock['INSERT_TS'] = now_asia
#    print(ltp_stock)
    # not req
    # index_list = nse.get_index_list()

    return ltp_stock


def india_vix():
    # india_vix = nse.get_index_list()
    india_vix = nse.get_index_quote("INDIA VIX")
    nifty_fifty = nse.get_index_quote("NIFTY 50")
    nifty_bank = nse.get_index_quote("NIFTY BANK")

    for i in india_vix:
        if i == "lastPrice":
            vix = india_vix[i]
            print(vix)

    for i in nifty_fifty:
        if i == "lastPrice":
            nifty_fifty = nifty_fifty[i]
            print(nifty_fifty)

    for i in nifty_bank:
        if i == "lastPrice":
            nifty_bank = nifty_bank[i]
            print(nifty_bank)

    y_vix = vix
    y_nifty_high = round(nifty_fifty*(1 + y_vix/100))
    y_nifty_low = round(nifty_fifty*(100 - y_vix)/100)

    m_vix = vix/math.sqrt(12)
    m_nifty_high = round(nifty_fifty*(1 + m_vix/100))
    m_nifty_low = round(nifty_fifty*(100 - m_vix)/100)

    w_vix = vix/math.sqrt(52)
    w_nifty_high = round(nifty_fifty*(1 + w_vix/100))
    w_nifty_low = round(nifty_fifty*(100 - w_vix)/100)

    d_vix = vix/math.sqrt(365)
    d_nifty_high = round(nifty_fifty*(1 + d_vix/100))
    d_nifty_low = round(nifty_fifty*(100 - d_vix)/100)

    index = pd.DataFrame({'INSERT_TS': now_asia, 'INDIA_VIX': [vix], 'NIFTY_FIFTY': [
        nifty_fifty], 'NIFTY_BANK': [nifty_bank],
        'Y_NIFTY_HIGH': [y_nifty_high], 'Y_NIFTY_LOW': [y_nifty_low],
        'M_NIFTY_HIGH': [m_nifty_high], 'M_NIFTY_LOW': [m_nifty_low],
        'W_NIFTY_HIGH': [w_nifty_high], 'W_NIFTY_LOW': [w_nifty_low],
        'D_NIFTY_HIGH': [d_nifty_high], 'D_NIFTY_LOW': [d_nifty_low]
    })
    print(index)
    return index


# def range_calculation {}

# top_gainers, top_losers = get_top_gainers_losers()
# write_to_csv(top_gainers, 'top_gainers')
# write_to_mysql(top_gainers, 'top_gainers', 'replace')
# write_to_csv(top_losers, 'top_losers')
# write_to_mysql(top_losers, 'top_losers', 'replace')


# india_vix = india_vix()
# write_to_mysql(india_vix, 'india_vix', 'replace')


# call function
print("Starting FnO Lot Size function")
fno_lot_size = fno_lot_sizes()
write_to_csv(fno_lot_size, 'fno_lot_size')
write_to_mysql(fno_lot_size, 'fno_lot_size', 'replace')
# write_to_gsheet
print('Writing to gsheet')
write_to_gsheet(fno_lot_size, 'gstocks-api', 0)

print("Starting LTP function")
ltp_stock = ltp_stock('FnO')
# output to csv
write_to_csv(ltp_stock, 'ltp_stock')
# output to mysql
write_to_mysql(ltp_stock, 'ltp_stock', 'replace')
# write_to_gsheet
print('Writing to gsheet')
write_to_gsheet(ltp_stock, 'gstocks-api', 1)



# print(fno_lot_size)




# Write to Gsheet
#write_to_mysql(ltp_stock, 'ltp_stock', 'replace')
#write_to_mysql(fno_lot_size, 'fno_lot_size', 'replace')

# q = nse.get_quote('LALPATHLAB')
# print(q)
