from nsepy import *
from datetime import date, timedelta
from output_module import write_to_mysql
import pandas as pd


# get current year
today = date.today()
today_year = today.year
today_month = today.month
month_list = range(1, today.month+1)
#month_list = range(1, 2)
#month_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


def get_expiry_dates():
    exp_list = []
    for month in month_list:
        expiry = get_expiry_date(year=today_year, month=month)
        max_expiry = max(expiry)
        exp_list.append(max_expiry)
#        print(exp_list)
    return exp_list


exp_list = get_expiry_dates()
print(exp_list)

expiry_df = pd.DataFrame(exp_list, columns=['EXPIRY_DATE'])

write_to_mysql(expiry_df, 'expiry_dates', 'replace')
