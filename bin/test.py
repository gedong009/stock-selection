import tushare as ts
import time
import sql_model
import pandas as pd
import common
from debug import p
import traceback
import datetime
import timeit

# df = ts.get_stock_basics()
# date = df.ix['600077']['timeToMarket']
# df = ts.get_stock_basics()
# date = df.ix['600848']['timeToMarket'] #上市日期YYYYMMDD
# print(date)
# exit()
# df = ts.get_h_data('600077', start='1991-12-01',end='2017-01-05',pause=10)
# # print(df)
# # exit()
# # df = ts.get_h_data(sCode, start=start, end=end)
# engine = create_engine('mysql://root:gedongSql@123@112.124.4.247/stock?charset=utf8')
# df['sCode'] = sCode
# df.rename(columns={'open': 'iOpeningPrice', 'high': 'iMaximumPrice',
#  'close': 'iClosingPrice', 'low': 'iMinimumPrice'},
#           inplace=True)
# #存入数据库
# df.to_sql('stock_daily_data', engine, if_exists='append')

# stock_data = ts.get_h_data('600077', start="2015-01-01", end="2016-01-10", autype='hfq')
# debug.p(stock_data)import tushare as ts


name = None
class test_class():
    def __init__(self):
        global name
        if name is None:
            name = 1
        else:
            name = name + 1
#
    def print(self):
        global name
        print(name)

#
# def __init__():
#     global name
#     if name is None:
#         name = 1
#         print(name)
#     else:
#         name = name + 1
#         print(name)
# #
# def print():
#     global name
#     print(name)
# def test2():
#     global name
#
#     print(name)
# test2()

