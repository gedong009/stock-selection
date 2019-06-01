# encoding: utf-8
import pymysql
import sql_model
import common
import time
import datetime
from sympy import *
import pandas as pd
import numpy as np

import debug


def get_macd_deviate(code):
    # 获取所有顶点
    loadDataSql = "select a.sCode, a.tApexDateTime, a.iApexDif, d.iMinimumPrice, a.iDirectionType" \
                  " from stock_daily_macd_apex a" \
                  " left join stock_daily_data d on a.tApexDateTime=d.tDateTime and a.sCode=d.sCode" \
                  " where a.sCode='" + code + "' " \
                                              " and a.iDirectionType=2" \
        # " and tApexDateTime='2014-03-03'"
    print(loadDataSql)
    conn = sql_model.get_conn()
    df = pd.read_sql(loadDataSql, conn)
    df['tApexDateTime'] = pd.to_datetime(df['tApexDateTime'])
    print("select ok")
    # row_array = sql_model.getAll(loadDataSql)


    # 获取该股票历史数据
    loadDataSql = "select * from stock_daily_data where sCode='" + code + "' "
    print(loadDataSql)
    h_data_df = pd.read_sql(loadDataSql, conn)
    h_data_df['tDateTime'] = pd.to_datetime(h_data_df['tDateTime'])

    # 最终结果列表
    data_list = []
    # 记录背离数据
    deviate = {}
    macd_data_deviate = pd.DataFrame()


    loadDataSql = "select * from stock_daily_macd_deviate where sCode='" + code + "' order by tDeviateDateTime desc limit 1"
    print(loadDataSql)
    newest_df = pd.read_sql(loadDataSql, conn)
    if len(newest_df) > 0:
        newest_df['tDeviateDateTime'] = pd.to_datetime(newest_df['tDeviateDateTime'])
        offset = df[df['tApexDateTime'] == newest_df.iloc[0]['tDeviateDateTime']].index[0]
        source_df = df.iloc[offset + 1:]
    else:
        source_df = df

    # for row in df.values:
    for index, row in source_df.iterrows():
        start = time.time()
        sCode = str(row['sCode'])
        tThisApexDateTime = row['tApexDateTime']
        iThisApexDif = row['iApexDif']
        iThisMinimumPrice = row['iMinimumPrice']
        iThisDirectionType = row['iDirectionType']
        # print(tThisApexDateTime)

        # 拿出9天内的最低点
        # print("sdf1")
        # withinDaysKLineApex = common.getWithinDaysKLineApex(sCode, tThisApexDateTime, iThisDirectionType, 9)
        withinDaysKLineApex = getWithinDaysKLineApex(h_data_df, tThisApexDateTime, iThisDirectionType)

        # end = time.time()
        # print("withinDaysKLineApex" + str(end - start))
        # print("sdf1")

        if withinDaysKLineApex is None:
            continue
        withinDaysKLineApexDate = withinDaysKLineApex['tDateTime']

        # 筛选第一步 先初步判断这个点是不是一个相对的一个顶点
        # print("sdf2")
        # beforDaysKLineApex = common.getBeforDaysKLineApex(sCode, tThisApexDateTime, iThisDirectionType, 40)
        beforDaysKLineApex = getBeforDaysKLineApex(h_data_df, tThisApexDateTime, iThisDirectionType, 40)
        # end = time.time()
        # print("beforDaysKLineApex" + str(end - start))
        # print("sdf2")
        if len(beforDaysKLineApex) < 1:
            continue
        beforDaysKLineApexDate = beforDaysKLineApex['tDateTime']

        # 如果40天内的顶点的日期 > (当前日期前后5天内的顶点)的日期。 那么这个点可能是个背离点
        # if common.strtotime(beforDaysKLineApexDate) > common.strtotime(withinDaysKLineApexDate):
        if beforDaysKLineApexDate > withinDaysKLineApexDate:
            continue
        # print(withinDaysKLineApexDate, beforDaysKLineApexDate)

        # 筛选第二步 获取 从前10个macd顶点里找 符合条件的顶点
        # 条件:
        #   1、当前日期dif > 历史日期dif (底背离)
        #   2、当前日期最低价 < 历史日期最低价 (底背离)
        #   注:顶背离相反
        # print("sdf3")
        # beforMacdApexArray = common.getBeforeMacdApex(sCode, tThisApexDateTime, iThisApexDif, iThisMinimumPrice,
        #                                               iThisDirectionType, 10)
        # 根据macd数据,提取背离起始点
        beforMacdApexDf = getBeforeMacdApex(df, tThisApexDateTime, iThisDirectionType, 10)
        # end = time.time()
        # print("beforMacdApexDf" + str(end - start))
        # print("sdf3")
        if len(beforMacdApexDf) < 1:
            continue
        # tmpBeginApex = []
        # print(beforMacdApexArray)

        # 筛选第三步 排除早期dif值较低的情况(顶背离相反)
        beforeApexApex = 0
        # for beforMacdApex in beforMacdApexDf:
        for index, row in beforMacdApexDf.iterrows():
            beginDate = str(row['tApexDateTime'])
            beginDif = row['iApexDif']

            # 如果起始点的最低价比当前最低价高则排除
            beginKLineApex = getWithinDaysKLineApex(h_data_df, beginDate, iThisDirectionType)
            if beginKLineApex['iMinimumPrice'] <= withinDaysKLineApex['iMinimumPrice']:
                continue
            # k线起始点的x y
            begin_kline_df = h_data_df[h_data_df['tDateTime'] == beginKLineApex['tDateTime']]
            x1 = begin_kline_df.index[0]
            y1 = begin_kline_df.iloc[0]['iMinimumPrice']
            # k线结束点的x y
            end_kline_df = h_data_df[h_data_df['tDateTime'] == withinDaysKLineApexDate]
            x2 = end_kline_df.index[0]
            y2 = end_kline_df.iloc[0]['iMinimumPrice']
            fc = equation_of_line(x1, y1, x2, y2)
            a = fc[0]
            b = fc[1]

            # 越界的次数
            ctb = 0
            # 根据直线方程,找出能够穿过直线的点
            for index2, row2 in h_data_df[x1 + 1:x2].iterrows():
                x = index2
                y = row2['iMinimumPrice']
                yline = a * x + b
                if yline - y > 0:
                    ctb += 1
                    # 超过3次则不算做背离

            # 超过3次 则判断不是背离点
            if ctb > 3:
                # debug.p(row2)
                continue

            # 满足条件 属于背离
            data_list.append({
                'sCode': sCode,
                'tBeginDateTime': beginDate,
                'iBeginDif': str(beginDif),
                'tDeviateDateTime': tThisApexDateTime,
                'iDeviateDif': str(iThisApexDif),
                'iDirectionType': str(iThisDirectionType),
            })
            # end = time.time()
            # print("data_list" + str(end - start))
            #
            # # 符合筛选第三步的进入
            # # if (beforeApexApex == 0 or beforeApexApex > beginDif) and iThisApexDif - beginDif > 0.3:
            # if beforeApexApex == 0 or beforeApexApex > beginDif:
            #     beforeApexApex = beginDif
            #
            #     # 筛选第四步 判断当前最低价日期是否是整个周期(历史macd低点的前后5日内最低价格的日期-->当前macd低点的前后5日内最低价格的日期)内最低的日期
            #     # print("sdf4")
            #     # beginWithinDaysKLineApex = common.getWithinDaysKLineApex(sCode, beginDate, iThisDirectionType)
            #     beginWithinDaysKLineApex = getBeforDaysKLineApex(h_data_df, beginDate, iThisDirectionType, 5)
            #
            #     end = time.time()
            #     print("beginWithinDaysKLineApex" + str(end - start))
            #     # print("sdf4")
            #     if beginWithinDaysKLineApex is None:
            #         continue
            #     # beginWithinDaysKLineDate: 开始的日期
            #     # withinDaysKLineApexDate: 当前K线顶点的日期
            #
            #     beginWithinDaysKLineDate = beginWithinDaysKLineApex[2]
            #     # print("sdf4")
            #     print(beginWithinDaysKLineDate)
            #     withinApex = common.getKLineApexByTime(sCode, beginWithinDaysKLineDate, withinDaysKLineApexDate,
            #                                            iThisDirectionType)
            #     debug.p(withinApex)
            #     # print("sdf5")
            #     # 顶点日期
            #     withinApexDate = withinApex[2]
            #     # print(withinDaysKLineApexDate, withinApexDate)
            #     # print(withinDaysKLineApexDate == withinApexDate)
            #
            #     if withinDaysKLineApexDate == withinApexDate:
            #         # 满足条件 属于背离
            #         data_list.append({
            #             'sCode': sCode,
            #             'tBeginDateTime': beginDate,
            #             'iBeginDif': str(beginDif),
            #             'tDeviateDateTime': tThisApexDateTime,
            #             'iDeviateDif': str(iThisApexDif),
            #             'iDirectionType': str(iThisDirectionType),
            #         })
            #         # dataList.append([sCode, beginDate, str(beginDif), tThisApexDateTime, str(iThisApexDif), str(iThisDirectionType)])

        end = time.time()
        print("end " + str(end - start), tThisApexDateTime)
    # debug.p(data_list)
    if len(data_list) > 0:
        macd_data_deviate = macd_data_deviate.append(data_list, ignore_index=True)
    return macd_data_deviate


def getWithinDaysKLineApex(df, datetime, direction_type=2, days=9):
    get_days = days - 1;
    if get_days % 2 == 0:
        beforeNum = int(get_days / 2)
    else:
        beforeNum = get_days % 2

    # debug.p(datetime)
    # df['tDateTime'] = df['tDateTime'].astype(str)
    # debug.p(df)
    # debug.p(type(datetime))
    this_index = df[df['tDateTime'] == datetime].index.values[0]
    begin_index = this_index - beforeNum
    begin_index = begin_index if begin_index > 0 else 0
    end_index = begin_index + days
    # test = this_df[begin_index:days]
    this_df = df[begin_index:end_index]
    # this_df = df[df['id'] == 1148700]
    if direction_type == 2:
        this_df = this_df.sort_values("iMinimumPrice", ascending=True).head(1)

    else:
        this_df = this_df.sort_values("iMaximumPrice", ascending=False).head(1)

    return this_df.loc[this_df.index.values[0]]


# 往前找{days}天, 返回顶点日期
def getBeforDaysKLineApex(df, datetime, direction_type=2, days=40):
    # debug.p(datetime)
    # df['tDateTime'] = df['tDateTime'].astype(str)
    # debug.p(df)
    # debug.p(type(datetime))
    # df.tDateTime = pd.to_datetime(df['tDateTime'])
    this_index = df[df['tDateTime'] == datetime].index.values[0]
    begin_index = this_index - days
    begin_index = begin_index if begin_index > 0 else 0
    end_index = this_index
    # test = this_df[begin_index:days]
    this_df = df[begin_index:end_index]
    if len(this_df) < 1:
        return []
    # this_df = df[df['id'] == 1148700]
    if direction_type == 2:
        this_df = this_df.sort_values("iMinimumPrice", ascending=True).head(1)
    else:
        this_df = this_df.sort_values("iMaximumPrice", ascending=False).head(1)

    return this_df.loc[this_df.index.values[0]]


# 根据macd数据,提取背离起始点
# 只是实现了底背离
def getBeforeMacdApex(df, apex_datetime, direction_type=2, count=10):
    # apex_datetime = pd.to_datetime('1994-02-02 00:00:00')
    # 将同一个方向的筛选出来
    direction_df = df[df['iDirectionType'] == direction_type]

    # 当天的数据
    this_df = direction_df[direction_df['tApexDateTime'] == apex_datetime]
    # 当天数据在direction_df内的index
    this_index = this_df.index.values[0]
    this_data = this_df.loc[this_index]

    # 当天数据的dif
    this_dif = this_data['iApexDif']
    # 当天数据的最低价
    this_minimum_price = this_data['iMinimumPrice']

    # begin_index = this_index - count
    # begin_index = begin_index if begin_index > 0 else 0
    # end_index = this_index

    end_date = apex_datetime

    # 筛选第二步 获取 从前10个macd顶点里找 符合条件的顶点
    # 条件:
    #   1、当前日期dif > 历史日期dif (底背离)
    #   2、当前日期最低价 < 历史日期最低价 (底背离)
    #   注:顶背离相反

    # 寻找当天之前的顶点
    before_macd_apex_df = df[df['tApexDateTime'] < end_date]
    # 去除价 or dif不符合
    deviate_begin_df = before_macd_apex_df[
        (before_macd_apex_df['iApexDif'] < this_dif) & (before_macd_apex_df['iMinimumPrice'] > this_minimum_price)]

    for index, row in deviate_begin_df.iterrows():
        # 两点之间直线方程式  y=ax+b
        x1 = index
        y1 = row['iApexDif']
        x2 = this_index
        y2 = this_data['iApexDif']
        fc = equation_of_line(x1, y1, x2, y2)
        a = fc[0]
        b = fc[1]

        # 越界的次数
        ctb = 0
        for index2, row2 in before_macd_apex_df[x1 + 1:x2].iterrows():
            x = index2
            y = row2['iApexDif']
            yline = a * x + b
            if yline - y > 0:
                ctb += 1
            # 超过1次则不算做背离
            if ctb > 1:
                break

        # 超过3次 将这个点删除
        if ctb > 0:
            deviate_begin_df = deviate_begin_df.drop(index)

    return deviate_begin_df


# 直线方程 返回a和b的值
def equation_of_line(x1, y1, x2, y2):
    # 两点之间直线方程式  y=ax+b
    # a = Symbol('a')
    # b = Symbol('b')
    # fc = solve([(y1 - a * x1) - b, (y2 - a * x2) - b], [a, b])
    r = np.linalg.solve([[x1, 1], [x2, 1]], [y1, y2])
    a = r.flat[0]
    b = r.flat[1]
    return [a, b]


# # effect_row = cursor.execute(loadDataSql)
# # historyApexRowArray = cursor.fetchall()
# loadDataSql = "select tApexDateTime, iApexDif, d.iMinimumPrice from stock_daily_macd_apex a " \
#               "left join stock_daily_data d on a.tApexDateTime=d.tDateTime and a.sCode=d.sCode " \
#               "where a.sCode='600077' and a.iDirectionType=2 and a.tApexDateTime < '"+tThisApexDateTime+"' and a.iApexDif < " + str(iThisApexDif) + " order by a.tApexDateTime desc limit 20"
# historyApexRowArray = sqlModel.getAll(loadDataSql)
# # 比当前更低的历史低点
# if len(historyApexRowArray):
#     # 获取当前日期的最低价
#     #  当前低点的dif
#     # loadDataSql = "select iMinimumPrice from stock_daily_data where sCode='600077' and tDateTime = '" + tApexDateTime + "'"
#     # iThisMinimumPrice = sqlModel.getOne(loadDataSql)
#     # iThisMinimumPrice = str(iThisMinimumPrice[0])
#     # print(iThisMinimumPrice)
#
#     historyDateTime = []
#     # 各个低点里的最低值
#     historyApexApex = 0
#     # print("tThisApexDateTime: " + tThisApexDateTime)
#     for historyApexRow in historyApexRowArray:
#         date = str(historyApexRow[0])
#         dif = historyApexRow[1]
#         iMinimumPrice = historyApexRow[2]
#         if historyApexApex == 0 or historyApexApex > dif:
#             historyApexApex = dif
#             v = 0 # 匹配的系数  当macd角度小时越大,当macd角度越大时越小
#             f = iThisApexDif - dif
#             p = iThisMinimumPrice - iMinimumPrice
#             if f < 0.3:
#                 # macd涨幅非常小
#                 v = 1
#             else:
#                 v = -0.5
#             # 之前的s点
#             s = 1 - (v * 0.01)
#             # print("dif: ", date, iThisApexDif, dif, iThisApexDif - dif)
#             # print(date, iMinimumPrice, float(iMinimumPrice) * s, iThisMinimumPrice)
#             if (float(iMinimumPrice) * s) > iThisMinimumPrice:
#                 # 满足条件 属于底背离
#                 dataList.append([sCode, date, str(dif), tThisApexDateTime, str(iThisApexDif), str(2)])
#
#             # if (f > 0.03):
#             #     print (iThisMinimumPrice / iMinimumPrice)
#                 # iMinimumPrice = iMinimumPrice - (iMinimumPrice * 0.01)
#             # print("price: ", date, iThisMinimumPrice, iMinimumPrice, iThisMinimumPrice - iMinimumPrice, iThisMinimumPrice / iMinimumPrice)
#             # historyDateTime.append(str(date))
#
#     # 为了避免macd相差很大, 价相差很小的情况
#     # searchMinimumPrice = str(iThisMinimumPrice-(iThisMinimumPrice*0.02))
#     # print(searchMinimumPrice)
#     # loadDataSql = "select tDateTime, iMinimumPrice from stock_daily_data " \
#     #               "where sCode='600077' and tDateTime in ('" + "','".join(historyDateTime) + "') " \
#     #               # "and iMinimumPrice > "+searchMinimumPrice
#     # historyDeviateRowArray = sqlModel.getAll(loadDataSql)
#     # print(loadDataSql)
#     # print(historyDateTime)
#     # print(historyDeviateRowArray)
#     # for h in historyDeviateRowArray:
#     #     print(h[0])
# for a in dataList:
#     print(a)
# exit()

engine = sql_model.get_conn()
sql = "select * from stock_basics order by code asc"
# sql = "select * from stock_basics where code > '601312' order by code asc"
# sql = "select * from stock_basics where code= '000002' order by code asc"
df = pd.read_sql(sql, engine)
code_list = df['code']

macd_data_deviate = pd.DataFrame()
for s in code_list:
    print(s + " begin ")
    macd_data_deviate = get_macd_deviate(s)
    if len(macd_data_deviate) > 0:
        # aField = ['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']
        result = sql_model.loadData('stock_daily_macd_deviate', macd_data_deviate.keys(), macd_data_deviate.values)
    print(s + " end ")

# aField = ['sCode', 'tBeginDateTime', 'iBeginDif', 'tDeviateDateTime', 'iDeviateDif', 'iDirectionType']
# result = sql_model.loadData('stock_daily_macd_deviate', aField, dataList)
# print(result)
