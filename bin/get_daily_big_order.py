#encoding: utf-8
import pymysql
import tushare as ts
import sql_model
import pandas as pd
import numpy as np
from debug import p
import threading
import code_redis
import datetime
import time
import debug


def dd(code, mysql_obj=None, threadName=None):
    now = datetime.datetime.now()

    # 周末退出
    if now.weekday() == 5 or now.weekday() == 6:
        return 1

    today = now.strftime("%Y-%m-%d")
    # today = '2018-07-06'

    while (True):
        df = ts.get_sina_dd(code, date=today)
        # df = ts.get_sina_dd(code, date='2018-07-06')
        # df = ts.get_sina_dd("000001", date='2018-07-05')
        # df = ts.get_sina_dd("000007", date=today)
        if df is not None and len(df) > 1:
            df['time'] = today + " " + df['time']
            df.rename(columns={'code': 'sCode', 'name': 'sName', 'time': 'tDateTime', 'price': 'iPrice',
                               'volume': 'iVolume', 'preprice': 'iPreprice', 'type': 'sType'}, inplace=True)
            df2 = df[['sCode', 'sName', 'tDateTime', 'iPrice', 'iVolume', 'iPreprice', 'sType']]
            # result = sql_model.loadData('stock_daily_big_order', df.keys, df.values, threadName)
            # result = sql_model.loadData('stock_daily_big_order', df2.keys(), df2.values, threadName)
            result = mysql_obj.loadData('stock_daily_big_order', df2.keys(), df2.values, threadName)
            print(result)

        # 过了15点10分就退出
        if (now.hour >= 15 and now.minute >= 10) or (now.hour < 9 and now.minute >= 30):
            return 1

        # 等待60秒
        print("%s wait 60s" % code)
        time.sleep(60)


# 获取所有股票
def get_data(threadName, mysql_obj):
    while 1 == 1:
        # 从列表去除
        code = code_redis.get_next_code("dd")
        # code = "601313"
        if code:
            print("%s: %s begin" % (threadName, code))
            dd(code, mysql_obj, threadName)
            print("%s: %s end" % (threadName, code))
        else:
            break

if __name__ == '__main__':
    # 将所有股票代码列入待获取列表中
    num = code_redis.reset_codelist_redis("dd")
    # 建立一个新数组
    threads = []
    # n = 2
    n = num
    counter = 1
    mysql_obj = sql_model.MysqlClass()
    while counter <= n:
        name = "Thread-" + str(counter)
        threads.append(threading.Thread(target=get_data, args=(name, mysql_obj,)))
        counter += 1
    # threads.append(thing5)
# 写个for让两件事情都进行
    for thing in threads:
        # setDaemon为主线程启动了线程matter1和matter2
        # 启动也就是相当于执行了这个for循环
        thing.setDaemon(True)
        thing.start()

    for thing in threads:
        thing.join()