#encoding: utf-8
import pymysql
import sql_model
import pandas as pd
import numpy as np
from debug import p
import threading
import code_redis
import debug

def buy(code, threadName=None):
    # loadDataSql = "select * " \
    #               "from stock_daily_macd_deviate d " \
    #               "left join stock_daily_data sd on sd.sCode = d.sCode " \
    #               "where d.iDirectionType=2 " \
    #               "and d.sCode='" + code + "' " \
    #               "and d.tDeviateDateTime>='2008-01-01' " \
    #               "and sd.iOpeningPrice is not null " \
    #               "group by d.tDeviateDateTime "
    #               # " and tApexDateTime='2017-04-11'"

    engine = sql_model.get_conn()
    loadDataSql = "SELECT d.id, d.sCode, d.tDeviateDateTime FROM stock_daily_macd_deviate d LEFT JOIN stock_daily_macd_deviate_buy buy ON buy.iDeviateId = d.id " \
          "WHERE d.sCode = '" + code + "' AND d.iDirectionType=2 AND buy.id IS NULL"
    # print(loadDataSql)
    df = pd.read_sql(loadDataSql, engine)
    if len(df) < 1:
        return
    # exit()
    # row_array = sql_model.getAll(loadDataSql)

    # for a in row_array:
    #     print(a)
    # exit()
    # 最终结果列表

    dataList = pd.DataFrame(columns=['sCode', 'iDeviateId', 'tDateTime'])
    for index, row in df.iterrows():
        tDeviateDateTime = str(row['tDeviateDateTime'])

        #确认买入点
        loadDataSql = "select m.tDateTime, d.iOpeningPrice, d.iClosingPrice " \
                      "from stock_daily_macd m " \
                      "left join stock_daily_data d on m.tDateTime=d.tDateTime and m.sCode=d.sCode " \
                      "where m.tDateTime > '" + tDeviateDateTime +"' " \
                      "and m.iBar > 0 " \
                      "and m.sCode='" + code + "' " \
                      "and d.iOpeningPrice is not null " \
                      "limit 1"
        # print(loadDataSql)
        buy_df = pd.read_sql(loadDataSql, engine)
        _dataList = pd.DataFrame([[code, str(row['id']), buy_df['tDateTime'][0]]], columns=['sCode', 'iDeviateId', 'tDateTime'])
        dataList = dataList.append(_dataList)

    result = sql_model.loadData('stock_daily_macd_deviate_buy', dataList.keys(), dataList.values, threadName)
    print(result)
    # p(dataList)
        # data_df =


# 获取所有股票
def get_data(threadName):
    while 1 == 1:
        # 从列表去除
        code = code_redis.get_next_code("buy")
        # code = "601313"
        if code:
            print("%s: %s begin" % (threadName, code))
            buy(code, threadName)
            print("%s: %s end" % (threadName, code))
        else:
            break

if __name__ == '__main__':
    # 将所有股票代码列入待获取列表中
    code_redis.reset_codelist_redis("buy")
    # 建立一个新数组
    threads = []
    # n = 10
    n = 10
    counter = 1
    while counter <= n:
        name = "Thread-" + str(counter)
        threads.append(threading.Thread(target=get_data, args=(name,)))
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
