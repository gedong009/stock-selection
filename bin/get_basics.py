import tushare as ts
from sqlalchemy import create_engine
import time
import sql_model
import common
import debug
# df = ts.get_stock_basics()
# date = df.ix['600077']['timeToMarket']
# df = ts.get_stock_basics()
# date = df.ix['600848']['timeToMarket'] #上市日期YYYYMMDD
# print(date)
# exit()
df = ts.get_stock_basics()
engine = create_engine('mysql://root:gedongSql@123@112.124.4.247/stock?charset=utf8')
# engine = sql_model.get_conn()
df.to_sql('stock_basics', engine, if_exists='append', index=True)
print(df)
exit()
# # df = ts.get_h_data(sCode, start=start, end=end)
# engine = create_engine('mysql://root:gedongSql@123@112.124.4.247/stock?charset=utf8')
# df['sCode'] = sCode
# df.rename(columns={'open': 'iOpeningPrice', 'high': 'iMaximumPrice',
#  'close': 'iClosingPrice', 'low': 'iMinimumPrice'},
#           inplace=True)
# #存入数据库
# df.to_sql('stock_daily_data', engine, if_exists='append')


# def get_h_data(code):
#     # engine = sql_model.get_conn()
#     # stock_data = ts.get_h_data(code, start="2017-01-01", end="2017-01-05", autype='hfq')
#     # stock_data['sCode'] = code
#     # stock_data['tDateTime'] = stock_data.index
#     # stock_data.rename(columns={'open': 'iOpeningPrice', 'high': 'iMaximumPrice', 'close': 'iClosingPrice',
#     #                            'low': 'iMinimumPrice', 'volume': 'iVolume', 'amount': 'iAmount'}, inplace=True)
#     # stock_data = common.get_average_line('600077', stock_data)
#     # stock_data2 = stock_data.sort_index(ascending=True)
#     # stock_data2.to_sql('stock_daily_data', engine, if_exists='append', index=False)
#     # debug.p(stock_data2)
#     # start_year = 1991
#     start_year = 1995
#     while start_year < 2019:
#         start = str(start_year) + "-01-01"
#         end_year = start_year + 1
#         end = str(end_year) + "-01-01"
#         print(start, end)
#         try:
#             fh = open("testfile", "w")
#             fh.write("这是一个测试文件，用于测试异常!!")
#             fh.close()
#             stock_data = ts.get_h_data(code, start=start, end=end, autype='hfq')
#             engine = sql_model.get_conn()
#             stock_data['sCode'] = code
#             stock_data['tDateTime'] = stock_data.index
#             stock_data2 = stock_data.sort_index(ascending=True)
#             stock_data2.rename(columns={'open': 'iOpeningPrice', 'high': 'iMaximumPrice', 'close': 'iClosingPrice',
#                                         'low': 'iMinimumPrice', 'volume': 'iVolume', 'amount': 'iAmount'}, inplace=True)
#             # 存入数据库
#             stock_data2.to_sql('stock_daily_data', engine, if_exists='append', index=False)
#
#             start_year = end_year
#         except IOError:
#             print("IOError等待60秒")
#             time.sleep(60)
#         else:
#             print(start, end, "成功")
#             time.sleep(20)
#
#
# get_h_data('600077')
