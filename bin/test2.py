import tushare as ts
import time
import sql_model
import pandas as pd
import common
import debug
import traceback
import datetime
from urllib.request import urlopen, Request, ProxyHandler, build_opener, install_opener
import requests
#
# a = requests.get("http://112.124.4.247:5010/get/").text
# print(a)
# exit()




# df = ts.get_h_data('600077', start='1916-12-01',end='2017-01-05', autype='hfq')
# print(df)
# exit()
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
# debug.p(stock_data)

def get_h_data(code):
    # engine = sql_model.get_conn()
    # stock_data = ts.get_h_data(code, start="2017-01-01", end="2017-01-05", autype='hfq')
    # stock_data['sCode'] = code
    # stock_data['tDateTime'] = stock_data.index
    # stock_data.rename(columns={'open': 'iOpeningPrice', 'high': 'iMaximumPrice', 'close': 'iClosingPrice',
    #                            'low': 'iMinimumPrice', 'volume': 'iVolume', 'amount': 'iAmount'}, inplace=True)
    # stock_data = common.get_average_line('600077', stock_data)
    # stock_data2 = stock_data.sort_index(ascending=True)
    # stock_data2.to_sql('stock_daily_data', engine, if_exists='append', index=False)
    # debug.p(stock_data2)
    # start_year = 1991
    # start_year = 1990
    # 从这个股票已有数据的最后一个日期开始获取
    engine = sql_model.get_conn()
    sql = "select * from stock_daily_data where sCode = " + code + " order by tDateTime desc limit 1"
    print(sql)
    df = pd.read_sql(sql, engine)
    if not df.empty:
        start_date = df.loc[0, ['tDateTime']].values[0] + datetime.timedelta(days=1)
        start_time = datetime.datetime.strptime(str(start_date), "%Y-%m-%d")
        # s = start_datetime.strftime("%Y-%m-%d")
        date_str = "2016-11-30 13:53:59"
        # datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        # start_year = t.year
    else:
        start_date = "1990-01-01"
        start_time = datetime.datetime.strptime(str(start_date), "%Y-%m-%d")

    while start_time < datetime.datetime.now():
        try:
            # fh = open("testfile", "w")
            # fh.write("这是一个测试文件，用于测试异常!!")
            # fh.close()

            # if start_time >= datetime.datetime.now():
            #     continue

            end_time = start_time + datetime.timedelta(days=365)
            # end = str(end_year) + "-01-01"
            # debug.p(start_time.year)
            print(code, start_time, end_time)


            stock_data = ts.get_h_data(code, start=str(start_time), end=str(end_time), autype='hfq')
            stock_data['sCode'] = code
            stock_data['tDateTime'] = stock_data.index
            stock_data2 = stock_data.sort_index(ascending=True)
            stock_data2.rename(columns={'open': 'iOpeningPrice', 'high': 'iMaximumPrice', 'close': 'iClosingPrice',
                                        'low': 'iMinimumPrice', 'volume': 'iVolume', 'amount': 'iAmount'}, inplace=True)
            # 存入数据库
            stock_data2.to_sql('stock_daily_data', engine, if_exists='append', index=False)

        except IOError:
            traceback.print_exc()
            # print("IOError等待60秒")
            # time.sleep(60)
            proxy_address = requests.get("http://112.124.4.247:5010/get/").text
            print("更换代理" + proxy_address)

            # 请求接口获取数据
            proxy = {
                # 'http': '106.46.136.112:808'
                # 'https': "https://112.112.236.145:9999",
                "http": proxy_address
            }
            print(proxy)
            # 创建ProxyHandler
            proxy_support = ProxyHandler(proxy)
            # 创建Opener
            opener = build_opener(proxy_support)
            # 安装OPener
            install_opener(opener)
        else:
            print(start_time, end_time, "成功")
            start_time = end_time + datetime.timedelta(days=1)

# 获取所有股票
engine = sql_model.get_conn()
sql = "select * from stock_basics"
df = pd.read_sql(sql, engine)
code_list = df['code']
for s in code_list:
    print(s + " begin ")
    get_h_data(s)
    print(s + " end ")
exit()

# 获取单个股票
get_h_data('600077')
