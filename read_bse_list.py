import requests
from bs4 import BeautifulSoup
import pandas as pd
from nse_tools_api import get_stock_symbol, get_top_gainers_losers, fno_lot_sizes, ltp_stock, india_vix
# from write_to_big_query_file import call_bigquery
# from write_to_big_query import call_bigquery_old
from input_module import read_from_csv
from output_module import write_to_csv, write_to_mysql
from ticker_finology import *
from functools import reduce

from datetime import datetime
from pytz import timezone
format = "%Y-%m-%d %H:%M:%S %Z%z"
# Current time in UTC
now_utc = datetime.now(timezone('UTC'))
now_asia = now_utc.astimezone(timezone('Asia/Kolkata'))

col_names = [
    "SECURITY_CODE",
    "ISSUER_NAME",
    "SECURITY_ID",
    "SECURITY_NAME",
    "STATUS",
    "GROUP",
    "FACE_VALUE",
    "ISIN_NO",
    "INDUSTRY",
    "INSTRUMENT"
]

filename = 'bse_listed_stocks.csv'


def get_bse_list():
    df_bse = read_from_csv(filename, col_names)
    # print(df_bse.head())
    return df_bse["SECURITY_ID"].tolist()


def get_bse_list_scrip():
    df_bse = read_from_csv(filename, col_names)
    df_bse["SECURITY_CODE"] = df_bse["SECURITY_CODE"].astype(
        str).str.slice(start=1)
    df_bse["SECURITY_CODE"] = 'SCRIP-1' + df_bse["SECURITY_CODE"].astype(str)
    # print(df_bse.head())
    return df_bse["SECURITY_CODE"].tolist() + df_bse["SECURITY_ID"].tolist()


#df_bse_list = get_bse_list_scrip()
# print(df_bse_list)
