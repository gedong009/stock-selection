#encoding: utf-8
import urllib.request
import json
import pymysql
import os
import shutil
import time
import sql_model
import pandas as pd
import debug

def get_macd(code):
    #快速平滑移动平均线EMA1的参数(日)
    short = 12
    #慢速平滑移动平均线EMA1的参数(日)
    long = 26
    #DIF的参数(日)
    m = 9

    loadDataSql = "select * from stock_daily_data where sCode='" + code + "' order by tDateTime"
    print(loadDataSql)
    conn = sql_model.get_conn()
    df = pd.read_sql(loadDataSql, conn)

    # 最终结果列表
    dataList = []
    # 前一天的内容
    before_data = []
    macd_data = pd.DataFrame()
    # df = df[['tDateTime', 'iOpeningPrice']]
    # df = df.set_index(['id'])
    # debug.p(df.head())

    for index, row in df.iterrows():
        id = index
        sCode = str(row['sCode'])
        tDateTime = str(row['tDateTime'])
        iClosingPrice = float(row['iClosingPrice'])

        if len(before_data):
            beforeEmaShort = float(before_data['iEmaShort'])
            beforeEmaLong = float(before_data['iEmaLong'])
            beforeDea = float(before_data['iDea'])
        else:
            beforeEmaShort = iClosingPrice
            beforeEmaLong = iClosingPrice
            beforeDea = 0
            # # 查询前一天的macd信息
            # getBeforeMacdSql = "select * from stock_daily_macd where sCode='" + sCode + "' ORDER BY tDateTime Desc limit 1"
            # before_df = pd.read_sql(getBeforeMacdSql, conn)
            # # cursorSub = conn.cursor()
            # # effect_row = cursorSub.execute(getBeforeMacdSql)
            # if not before_df.empty:
            # # if effect_row != 0:
            # #     subRow = cursorSub.fetchone()
            #     subRow = before_df.head(1)
            #     beforeEmaShort = float(subRow['iEmaShort'])
            #     beforeEmaLong = float(subRow['iEmaLong'])
            #     beforeDea = float(subRow['iDea'])
        # print(beforeEmaShort, beforeEmaLong, beforeDea)
        # EMA（12）=前一日EMA（12）×11/13＋今日收盘价×2/13
        emaShort = round(beforeEmaShort*(short-1)/(short+1) + iClosingPrice*2/(short + 1), 3)
        # EMA（26）=前一日EMA（26）×25/27＋今日收盘价×2/27
        emaLong = round(beforeEmaLong*(long-1)/(long+1) + iClosingPrice*2/(long+1), 3)
        # DIF=今日EMA（12）－今日EMA（26）
        dif = round(emaShort - emaLong, 3)
        # 今日DEA（MACD）=前一日DEA×8 / 10＋今日DIF×2 / 10
        dea = round(beforeDea*(m-1)/(m+1) + dif*2/(m+1), 3)
        # BAR=2×(DIF－DEA)
        bar = round(2 * (dif-dea), 3)

        # beforeData = [sCode, tDateTime, str(emaShort), str(emaLong), str(dif), str(dea), str(bar)]

        before_data = {
            'sCode': sCode,
            'tDateTime': tDateTime,
            'iEmaShort': str(emaShort),
            'iEmaLong': str(emaLong),
            'iDif': str(dif),
            'iDea': str(dea),
            'iBar': str(bar),
        }

        df.at[id, 'iEmaShort'] = emaShort
        df.at[id, 'iEmaLong'] = emaLong
        df.at[id, 'iDif'] = dif
        df.at[id, 'iDea'] = dea
        df.at[id, 'iBar'] = bar
        # print(before_data)
        # row = pd.DataFrame(before_data, index=[0])
        # print(end - start)
        # print(totle_time)
        # # debug.p(row)
        # macd_data = macd_data.append(row, ignore_index=True)
        # debug.p(macd_data)
        # dataList.append(beforeData)

    # df = df[['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']]
    print(totle_time)
    return df

# for d in dataList:
#     print(d)

# print(dataList)
# macd_data = get_macd('600077')
# 获取所有股票
engine = sql_model.get_conn()
sql = "select * from stock_basics order by code asc"
# sql = "select * from stock_basics where code='000001' order by code asc"
df = pd.read_sql(sql, engine)
code_list = df['code']

macd_data = pd.DataFrame()
for s in code_list:
    print(s + " begin ")
    totle_time = 0
    start = time.time()
    macd_data = get_macd(s)
    macd_data = macd_data[['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']]
    # debug.p(macd_data)
    end = time.time()
    totle_time += end - start
    print(totle_time)
    # aField = ['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']
    result = sql_model.loadData('stock_daily_macd', macd_data.keys(), macd_data.values)
    print(s + " end ")
#
# debug.p(macd_data)
# # result = sql_model.loadData('stock_daily_macd', aField, dataList)
# aField = ['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']
# result = sql_model.loadData('stock_daily_macd', macd_data.keys(), macd_data.values)
#
# print(result)


