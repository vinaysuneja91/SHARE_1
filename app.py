from telegram import telegram_bot_sendtext
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from nse_tools_api import get_stock_symbol, get_top_gainers_losers, fno_lot_sizes, ltp_stock, india_vix
# from write_to_big_query_file import call_bigquery
# from write_to_big_query import call_bigquery_old
from input_module import read_from_csv, read_from_mysql
from output_module import write_to_csv, write_to_mysql
from ticker_finology import *
from functools import reduce
from read_bse_list import get_bse_list, get_bse_list_scrip
from input_module import read_from_mysql
from datetime import datetime, date
from pytz import timezone
format = "%Y-%m-%d %H:%M:%S %Z%z"
from date_time_module import now_asia
# Current time in UTC
#now_utc = datetime.now(timezone('UTC'))
#now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))

# finology_html, finology_overall_rating, finology_52_week_high_low,
# finology_ltp, finology_ratios


# query_string = query_string = """delete from `smartmeteranalytics.TEST.T2` where 1=1"""
# call_bigquery_old(query_string)
# print('Table Truncated')

print("Starting Program")
bse_list1 = get_bse_list()
# bse_list2, bse_stock_symbol_list = get_bse_list_scrip()
bse_list2 = get_bse_list_scrip()

bse_list = bse_list2 + bse_list1
# print("Total records in bse -" + str(len(bse_list)))

list1 = []

nse_list = get_stock_symbol()

#query_result = read_from_mysql(
#    'select SCRIP from iwu_master where SCRIP is not null')
#query_df = pd.DataFrame(query_result, columns=['SCRIP'])

# print(df)

#IWU_SCRIP_LIST = query_df['SCRIP'].tolist()
#print(IWU_SCRIP_LIST)


# nse_list = []
print("Total records in nse -" + str(len(nse_list)))
#print("Total records in IWU List -" + str(len(IWU_SCRIP_LIST)))
print("Total records in bse -" + str(len(bse_list)))

# stock_list = bse_list1 + bse_list2 + nse_list
#stock_list = bse_list2[:5] + nse_list[:5]
# stock_list = bse_list2 + nse_list
# stock_list = nse_list + IWU_SCRIP_LIST + bse_list
stock_list = nse_list
# stock_list = bse_list1
# stock_list = IWU_SCRIP_LIST
# stock_list = ['CHEMCON']

#stock_list = stock_list[:5]
# stock_list = ['RBLBANK']

stock_list = list(dict.fromkeys(stock_list))

total_api_calls = len(stock_list)

# stock_list = stock_list[:50]
# stock_symbol = bse_list2
# sampling
# stock_list = ['BAJFINANCE', 'SCRIP-100034']


# writing stocklist into master table
print('writing stock list to master table')
stock_list_df = pd.DataFrame(stock_list, columns=['STOCK_SYMBOL'])
index_list = ['NIFTY', 'BANKNIFTY']  # , 'NIFTY FIN SERVICE']
index_list_df = pd.DataFrame(index_list)
#stock_list_df = pd.DataFrame(index_list, columns=['STOCK_SYMBOL'])
stock_list_df = stock_list_df.append(index_list_df)
write_to_mysql(stock_list_df, 't_stock_symbol_master', 'replace')

# writing fno stocklist into fno master table
#print('writing fno stock list to master table')
#fno_lot_size = fno_lot_sizes()
#print(fno_lot_size)


# Starting finology
print("Total Api calls -" + str(total_api_calls))


for stock in stock_list:
    rating_final = '  '
    rating = ''
    tup1 = ()
    stock_symbol = stock
    # bse_stock_symbol = bse_stock_symbol_list[bse_stock_symbol_list.index(
    #    stock_symbol)]
    # print(bse_stock_symbol)

    # stock_name = stock_symbol[0]
    print("Stock running for - " + stock_symbol)
    url, soup = finology_html(stock_symbol)

    rating_final, valuation_rating, ownership_rating, efficiency_rating, financial_rating = finology_overall_rating(
        soup)
    # print(rating_final,valuation_rating)

    _52_high, _52_low = finology_52_week_high_low(soup)
    # print(rating_final)

    ltp = finology_ltp(soup)

    sales_growth = finology_ratios(soup, "#mainContent_divSales")
    # print(sales_growth)

    profit_growth = finology_ratios(soup, "#mainContent_divProfit")
    # print(profit_growth)

    roe = finology_ratios(soup, "#mainContent_divROE")
    # print(roe)

    roce = finology_ratios(soup, "#mainContent_divROCE")
    # print(roce)

    sector = finology_sector(soup)
    # print(sector)

    market_cap, div_yield, debt, promoter_holding = finology_promoter_holding(
        soup)
    # print(market_cap, promoter_holding)
    icr = finology_icr(soup)

    pledged_holding = finology_pledged_holing(soup)

    stock_name = finology_stock_name(soup)
    # print(stock_name)
    peg_ratio = finology_peg_ratio(soup)
    print(peg_ratio)

    cash_flow_trend, latest_cash_flow, cash_flow_minus_1, cash_flow_minus_2, cash_flow_minus_3, cash_flow_minus_4 = finology_operating_cash_flow(
        soup)

    cash_flow_5_years = [latest_cash_flow, cash_flow_minus_1,
                         cash_flow_minus_2, cash_flow_minus_3, cash_flow_minus_4]
    # print(cash_flow_5_years)
    # discounted_rate = 25
    net_profit_trend, latest_net_profit, net_profit_minus_1, net_profit_minus_2, net_profit_minus_3, net_profit_minus_4 = finology_net_profit(
        soup)


# combining columns together
    tup1 = (now_asia, sector, stock_symbol, stock_name, market_cap, div_yield, debt, icr, promoter_holding, pledged_holding,
            rating_final, valuation_rating, ownership_rating, efficiency_rating, financial_rating, _52_high, _52_low, ltp,
            sales_growth[0], sales_growth[1], sales_growth[2],
            profit_growth[0], profit_growth[1], profit_growth[2],
            roe[0], roe[1], roe[2],
            roce[0], roce[1], roce[2],
            cash_flow_trend, latest_cash_flow, cash_flow_minus_1, cash_flow_minus_2, cash_flow_minus_3, cash_flow_minus_4,
            net_profit_trend, latest_net_profit, net_profit_minus_1, net_profit_minus_2, net_profit_minus_3, net_profit_minus_4,
            peg_ratio, url)
    print(tup1)
    list1.append(tup1)
    print("Stocks completed so far -> " + str(len(list1)))
    print("Stocks remaining so far -> " + str(total_api_calls - len(list1)))
    print("% Complete -> " + str(round(len(list1)*100 / total_api_calls, 2)) + '%')

# print("appended_list")
# print(list1)


data_out = pd.DataFrame(
    list1, columns=['INSERT_TS', 'SECTOR', 'STOCK_SYMBOL', 'STOCK_NAME', 'MARKET_CAP_CR', 'DIV_YIELD', 'DEBT', 'ICR', 'PROMOTER_HOLDING', 'PLEDGED_HOLDING',  'RATING', 'VALUATION_RATING', "OWNERSHIP_RATING", 'EFFICIENCY_RATING', 'FINANCIAL_RATING', '_52_W_HIGH', '_52_W_LOW', 'LTP',
                    'SG_1_YR', 'SG_3_YR', 'SG_5_YR',
                    'PG_1_YR', 'PG_3_YR', 'PG_5_YR',
                    'ROE_1_YR', 'ROE_3_YR', 'ROE_5_YR',
                    'ROCE_1_YR', 'ROCE_3_YR', 'ROCE_5_YR',
                    'CASH_FLOW_TREND', 'LATEST_CASH_FLOW',
                    'CASH_FLOW_MINUS_1', 'CASH_FLOW_MINUS_2',
                    'CASH_FLOW_MINUS_3', 'CASH_FLOW_MINUS_4',
                    'NET_PROFIT_TREND', 'LATEST_NET_PROFIT',
                    'NET_PROFIT_MINUS_1', 'NET_PROFIT_MINUS_2',
                    'NET_PROFIT_MINUS_3', 'NET_PROFIT_MINUS_4',
                    'PEG_RATIO', 'URL'])

data_out[['MARKET_CAP_CR']] = data_out[['MARKET_CAP_CR']].apply(pd.to_numeric)
data_out[['DIV_YIELD']] = data_out[['DIV_YIELD']].apply(pd.to_numeric)
data_out[['DEBT']] = data_out[['DEBT']].apply(pd.to_numeric)
data_out[['PEG_RATIO']] = data_out[['PEG_RATIO']].apply(pd.to_numeric)
data_out[['ICR']] = data_out[['ICR']].apply(pd.to_numeric)
data_out[['PROMOTER_HOLDING']] = data_out[[
    'PROMOTER_HOLDING']].apply(pd.to_numeric)
data_out[['PLEDGED_HOLDING']] = data_out[[
    'PLEDGED_HOLDING']].apply(pd.to_numeric)
data_out[['RATING']] = data_out[['RATING']].apply(pd.to_numeric)
data_out[['VALUATION_RATING']] = data_out[[
    'VALUATION_RATING']].apply(pd.to_numeric)
data_out[['OWNERSHIP_RATING']] = data_out[[
    'OWNERSHIP_RATING']].apply(pd.to_numeric)
data_out[['EFFICIENCY_RATING']] = data_out[[
    'EFFICIENCY_RATING']].apply(pd.to_numeric)
data_out[['FINANCIAL_RATING']] = data_out[[
    'FINANCIAL_RATING']].apply(pd.to_numeric)
data_out[['_52_W_HIGH']] = data_out[['_52_W_HIGH']].apply(pd.to_numeric)
data_out[['_52_W_LOW']] = data_out[['_52_W_LOW']].apply(pd.to_numeric)
data_out[['LTP']] = data_out[['LTP']].apply(pd.to_numeric)
data_out[['LATEST_CASH_FLOW']] = data_out[[
    'LATEST_CASH_FLOW']].apply(pd.to_numeric)
data_out[['CASH_FLOW_MINUS_1']] = data_out[[
    'CASH_FLOW_MINUS_1']].apply(pd.to_numeric)
data_out[['CASH_FLOW_MINUS_2']] = data_out[[
    'CASH_FLOW_MINUS_2']].apply(pd.to_numeric)
data_out[['CASH_FLOW_MINUS_3']] = data_out[[
    'CASH_FLOW_MINUS_3']].apply(pd.to_numeric)
data_out[['CASH_FLOW_MINUS_4']] = data_out[[
    'CASH_FLOW_MINUS_4']].apply(pd.to_numeric)
data_out[['LATEST_NET_PROFIT']] = data_out[[
    'LATEST_NET_PROFIT']].apply(pd.to_numeric)
data_out[['NET_PROFIT_MINUS_1']] = data_out[[
    'NET_PROFIT_MINUS_1']].apply(pd.to_numeric)
data_out[['NET_PROFIT_MINUS_2']] = data_out[[
    'NET_PROFIT_MINUS_2']].apply(pd.to_numeric)
data_out[['NET_PROFIT_MINUS_3']] = data_out[[
    'NET_PROFIT_MINUS_3']].apply(pd.to_numeric)
data_out[['NET_PROFIT_MINUS_4']] = data_out[[
    'NET_PROFIT_MINUS_4']].apply(pd.to_numeric)


data_out[['PG_1_YR']] = data_out[['PG_1_YR']].apply(pd.to_numeric)
data_out[['PG_3_YR']] = data_out[['PG_3_YR']].apply(pd.to_numeric)
data_out[['PG_5_YR']] = data_out[['PG_5_YR']].apply(pd.to_numeric)

data_out[['SG_5_YR']] = data_out[['SG_1_YR']].apply(pd.to_numeric)
data_out[['SG_5_YR']] = data_out[['SG_3_YR']].apply(pd.to_numeric)
data_out[['SG_5_YR']] = data_out[['SG_5_YR']].apply(pd.to_numeric)

data_out[['ROE_5_YR']] = data_out[['ROE_1_YR']].apply(pd.to_numeric)
data_out[['ROE_5_YR']] = data_out[['ROE_3_YR']].apply(pd.to_numeric)
data_out[['ROE_5_YR']] = data_out[['ROE_5_YR']].apply(pd.to_numeric)

data_out[['ROCE_1_YR']] = data_out[['ROCE_1_YR']].apply(pd.to_numeric)
data_out[['ROCE_3_YR']] = data_out[['ROCE_3_YR']].apply(pd.to_numeric)
data_out[['ROCE_5_YR']] = data_out[['ROCE_5_YR']].apply(pd.to_numeric)


# data_out = data_out[data_out.SECTOR.notnull()]
# data_out = data_out[data_out.SECTOR != 'na']


data_out.sort_values("STOCK_NAME", inplace=True)


# dropping ALL duplicte values
data_out.drop_duplicates(subset="STOCK_NAME",
                         keep="first", inplace=True)

data_out = data_out.sort_values('MARKET_CAP_CR', ascending=False)


# data_out["SNO"] = np.arange(len(data_out))
# data_out['SNO'] = data_out.reset_index().index
data_out.insert(loc=0, column='SNO', value=data_out.reset_index().index + 1)

total_rows = len(data_out.index)
print("Total Records in df-", total_rows)

# output to csv
# df_data.to_csv(r'output.csv', index=False)
write_to_csv(data_out, 'stock_detail')

# output to mysql
write_to_mysql(data_out, 'stocks_detail', 'replace')
# write_to_mysql(data_out, 'stocks_detail_bse', 'replace')

# output to bigquery
# print(call_bigquery())


# Calling nse api for top gainers and losers and saving it in a file
# top_gainers, top_losers = get_top_gainers_losers()
# write_to_csv(top_gainers, 'top_gainers')
# write_to_csv(top_losers, 'top_losers')

# ltp_stock
#ltp_stock = ltp_stock()
#write_to_mysql(ltp_stock, 'ltp_stock', 'replace')


india_vix = india_vix()
write_to_mysql(india_vix, 'india_vix', 'replace')


# uploading my watchlist
filename = "watchlist_codes.csv"
# username = filename[0:6]

watchlist_df = read_from_csv(filename)

watchlist_df_filtered = watchlist_df[watchlist_df['SUB_SOURCE'] != 'X']

print(watchlist_df_filtered.head())

# write to mysql
write_to_mysql(watchlist_df_filtered, 'my_watchlist', 'replace')


# send message to telegram group
# read stocks from mysql
today = date.today()
text = f"Good Stocks at Value Prices for {today}"
print(text)
test = telegram_bot_sendtext(text)
print(test)

value_stocks_list = read_from_mysql(
    # "SELECT GROUP_CONCAT(STOCK_SYMBOL,' Rs-',round(LTP,0)) AS 'STOCKS' from V_FINOLOGY_BEST")
    "SELECT CONCAT(STOCK_SYMBOL,' @',round(LTP,0)) AS 'STOCKS' from V_FINOLOGY_BEST LIMIT 5")
print(value_stocks_list)
res = [''.join(i) for i in value_stocks_list]
print(res)
for text in res:
    print(text)
    test = telegram_bot_sendtext(text)
    print(test)

    # str1 = " "
    # text = str1.join(res)
    # print(text)
