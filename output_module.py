import pandas as pd
import mysql.connector
from pandas.io import sql
from sqlalchemy import create_engine


def write_to_csv(df_data, filename):
    print('writing to ouput file')
    file = f'{filename}.csv'
    location = f'output\{file}'
    df_data.to_csv(location, index=True
                   )


user = 'stock_user'
passw = 'stock_user'
#host = '192.168.0.5'
#host = '192.168.1.10'
host = 'localhost'
port = 3306
database = 'MYSTOCKS'


def write_to_mysql(df, table_name, if_exists):
    print('write to mysql')
    engine = create_engine(
        f'mysql+mysqlconnector://{user}:{passw}@{host}:{port}/{database}', echo=False)
    df.to_sql(name=table_name, con=engine,
              if_exists=if_exists, index=False
              )


def read_from_mysql(query):
    print('read from mysql')
    engine = create_engine(
        f'mysql+mysqlconnector://{user}:{passw}@{host}:{port}/{database}', echo=False)
    df.to_sql(name=table_name, con=engine,
              if_exists=if_exists, index=False
              )


