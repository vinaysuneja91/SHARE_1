import pandas as pd
import mysql.connector


# names = ["posting_date", "cost_center", "remarks",
#         "voucher_type", "debit", "credit", "net_balance"]


def read_from_csv(filename, col_names=[]):
    # print(col_names)
    if not col_names:
        print('no_col_names_passed')
        location = f'input\{filename}'
        data = pd.read_csv(
            location, skiprows=0  # , header=None  # , dtype=dtype
            # , names=col_names  # , parse_dates=["posting_date"]
        )
    else:
        location = f'input\{filename}'
        data = pd.read_csv(
            location, skiprows=1  # , header=None  # , dtype=dtype
            , names=col_names  # , parse_dates=["posting_date"]
        )
    return data


def read_from_mysql(query_string):
    mydb = mysql.connector.connect(
        user='stock_user',
        password='stock_user',
        #host = '192.168.0.5'
        host='127.0.0.1',
        port=3306,
        database='MYSTOCKS'
    )
    mycursor = mydb.cursor()
    mycursor.execute(query_string)
    myresult = mycursor.fetchall()
    # print(myresult)
    # the above results a list of tuples if we use below code it will result a list
    #query_list = list(sum(query_result, ()))
    return myresult


#def read_from_mysql(query_string):
#    mydb = mysql.connector.connect(
#        user='stock_user',
#        password='stock_user',
#        #host = '192.168.0.5'
#        host='127.0.0.1',
#        port=3306,
#        database='MYSTOCKS'
#    )
#    mycursor = mydb.cursor()
#    mycursor.execute(query_string)
#    myresult = mycursor.fetchall()
#    # print(myresult)
#    # the above results a list of tuples if we use below code it will result a list
#    #query_list = list(sum(query_result, ()))
#    return myresult

def exe_in_mysql(query_string):
    print(query_string)
    mydb = mysql.connector.connect(
        user='stock_user',
        password='stock_user',
        #host = '192.168.0.5'
        host='127.0.0.1',
        port=3306,
        database='MYSTOCKS'
    )
    mycursor = mydb.cursor()
    mycursor.execute(query_string)
    mydb.commit()
#   myresult = mycursor.fetchall()
    # print(myresult)
    # the above results a list of tuples if we use below code it will result a list
    #query_list = list(sum(query_result, ()))
    mydb.close()
    return 'success'
