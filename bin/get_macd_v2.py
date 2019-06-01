# encoding: utf-8
import urllib.request
import json
import pymysql
import os
import shutil
import time
import sql_model
import pandas as pd
import numpy as np
from debug import p
import debug


def get_EMA(df, N):
    ema = []
    for index, row in df.iterrows():
        # for i in range(len(df)):
        if index == 0:
            # 当是第一个时有两个可能
            # 1: 没有历史数据从头开始, ema使用当天收盘价
            # 2: 有历史数据,数据迭代, ema使用最后一天的ema
            if 'ema' not in list(df) or np.isnan(df['ema'][0]):
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

    # 列表内有iEmaShort列并且第一行不为空, 将它的值给到ema
    if 'iEmaShort' in list(df) and not np.isnan(df['iEmaShort'][0]):
        df.at[0, 'ema'] = df.ix[0, 'iEmaShort']
    # p(df)
    a = get_EMA(df, short)

    # 列表内有iEmaLong列并且第一行不为空, 将它的值给到ema
    if 'iEmaLong' in list(df) and not np.isnan(df['iEmaLong'][0]):
        df.at[0, 'ema'] = df.ix[0, 'iEmaLong']
    b = get_EMA(df, long)

    # DIF=今日EMA（12）－今日EMA（26）
    df['iDif'] = pd.Series(a) - pd.Series(b)
    df['iEmaShort'] = pd.Series(a)
    df['iEmaLong'] = pd.Series(b)
    # print(df['iDif'])
    # for i in range(len(df)):
    for index, row in df.iterrows():
        if index == 0:
            if 'iDea' not in list(df) or np.isnan(df['iDea'][0]):
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
# sql = "select * from stock_basics where code = '002030' order by code asc"
df = pd.read_sql(sql, engine)
code_list = df['code']

macd_data = pd.DataFrame()
for s in code_list:
    print(s + " begin ")
    # macd_data = get_macd(s)

    conn = sql_model.get_conn()

    # 原代码
    # loadDataSql = "select * from stock_daily_data where sCode='" + s + "' order by tDateTime"
    # 修改开始
    # loadDataSql = "select * from stock_daily_data where sCode='" + s + "' order by tDateTime limit 15"
    # # 修改结束
    # print(loadDataSql)
    # df = pd.read_sql(loadDataSql, conn)
    # macd_data = get_MACD(df, 12, 26, 9)
    # macd_data = macd_data[['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']]


    # loadDataSql = "select * from stock_daily_macd where sCode='" + s + "' and tDateTime='1991-04-26'"
    loadDataSql = "select * from stock_daily_macd where sCode='" + s + "' order by tDateTime desc limit 1"
    print(loadDataSql)
    history_macd_df = pd.read_sql(loadDataSql, conn)

    # 判断是否有历史数据
    if not history_macd_df.empty:
        # 从指定日期获取股票数据
        tStartTime = history_macd_df.at[0, 'tDateTime']
        loadDataSql = "select * from stock_daily_data where sCode='" + s + "' and tDateTime>='" + str(
            tStartTime) + "' order by tDateTime"
    else:
        # 从头获取股票数据
        loadDataSql = "select * from stock_daily_data where sCode='" + s + "' order by tDateTime"

    print(loadDataSql)
    df = pd.read_sql(loadDataSql, conn)
    if df.empty:
        continue

    # 判断是否有历史数据
    if not history_macd_df.empty:
        df.at[0, 'iEmaShort'] = history_macd_df.ix[0, 'iEmaShort']
        df.at[0, 'iEmaLong'] = history_macd_df.ix[0, 'iEmaLong']
        df.at[0, 'iDif'] = history_macd_df.ix[0, 'iDif']
        df.at[0, 'iDea'] = history_macd_df.ix[0, 'iDea']
        df.at[0, 'iBar'] = history_macd_df.ix[0, 'iBar']
    macd_data = get_MACD(df, 12, 26, 9)
    macd_data = macd_data[['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']]


    # p(macd_data)
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
