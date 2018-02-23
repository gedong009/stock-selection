# encoding: utf-8
import urllib.request
import json
import pymysql
import os
import shutil
import time
import sql_model
import pandas as pd
import debug


def get_EMA(df, N):
    ema = []
    for index, row in df.iterrows():
        # for i in range(len(df)):
        if index == 0:
            df.at[index, 'ema'] = row['iClosingPrice']
        if index > 0:
            # EMA（12）=前一日EMA（12）×11/13＋今日收盘价×2/13
            # EMA（26）=前一日EMA（26）×25/27＋今日收盘价×2/27
            df.at[index, 'ema'] = df.ix[index - 1, 'ema'] * (N - 1) / (N + 1) + row['iClosingPrice'] * 2 / (N + 1)
    ema = list(df['ema'])
    return ema


def get_MACD(df, short=12, long=26, M=9):
    start = time.time()
    totle_time = 0
    a = get_EMA(df, short)
    b = get_EMA(df, long)
    # DIF=今日EMA（12）－今日EMA（26）
    df['iDif'] = pd.Series(a) - pd.Series(b)
    df['iEmaShort'] = pd.Series(a)
    df['iEmaLong'] = pd.Series(b)
    # print(df['iDif'])
    # for i in range(len(df)):
    for index, row in df.iterrows():
        if index == 0:
            df.at[index, 'iDea'] = row['iDif']
        if index > 0:
            # 今日DEA（MACD）=前一日DEA×8 / 10＋今日DIF×2 / 10
            df.at[index, 'iDea'] = df.ix[index - 1, 'iDea'] * (M - 1) / (M + 1) + row['iDif'] * 2 / (M + 1)

    # BAR=2×(DIF－DEA)
    end = time.time()
    totle_time += end - start
    print(totle_time)
    df['iBar'] = 2 * (df['iDif'] - df['iDea'])
    return df


# 获取所有股票
engine = sql_model.get_conn()
sql = "select * from stock_basics order by code asc"
# sql = "select * from stock_basics where code='000001' order by code asc"
df = pd.read_sql(sql, engine)
code_list = df['code']

macd_data = pd.DataFrame()
for s in code_list:
    print(s + " begin ")
    # macd_data = get_macd(s)
    loadDataSql = "select * from stock_daily_data where sCode='" + s + "' order by tDateTime"
    print(loadDataSql)
    conn = sql_model.get_conn()
    df = pd.read_sql(loadDataSql, conn)
    macd_data = get_MACD(df, 12, 26, 9)
    macd_data = macd_data[['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']]

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
