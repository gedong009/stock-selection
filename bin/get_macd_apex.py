# encoding: utf-8
import pymysql
import sql_model
import pandas as pd
import time
import debug


def get_mack_apex(code):
    # 打开数据库连接
    conn = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock',
                           charset='utf8')

    # 创建游标
    cursor = conn.cursor()
    # 执行SQL，并返回收影响行数
    # loadDataSql = "load data local infile '" + mysqlFilePath + "' ignore into table stock_daily_data;"
    loadDataSql = "select sCode, tDateTime, iDif, iBar from stock_daily_macd where sCode='" + code + "' order by tDateTime"
    # loadDataSql = "select sCode, tDateTime, iDif, iBar from stock_daily_macd where sCode='" + code + "' and tDateTime > '1993-04-20' and tDateTime < '1993-08-01'  order by tDateTime"
    print(loadDataSql)
    conn = sql_model.get_conn()
    df = pd.read_sql(loadDataSql, conn)
    row_array = sql_model.getAll(loadDataSql)

    # 最终结果列表
    data_list = []
    # 斜率系数
    slope = 0.05
    # 记录顶点数据
    apex = {}
    macd_data_apex = pd.DataFrame()
    totle_time = 0
    # for row in df.values:
    for index, row in df.iterrows():
        start = time.time()
        sCode = str(row['sCode'])
        tDateTime = str(row['tDateTime'])
        thisDif = row['iDif']
        thisBar = row['iBar']

        # 是否有顶点数据
        if 'tApexDateTime' in apex.keys():

            # 计算斜率k = (y2 - y1) / (x2 - x1) 由于我们是按天算的分母肯定是1 这里乘以8确保和y轴相对平衡
            direction = apex['iDirectionType']
            # 当前dif - 顶点dif
            k = thisDif - apex['iApexDif']

            # 当 在非上升趋势中,出现当前dif比最低dif高了一个系数则认为是改变了向上
            if k > slope and direction != 1 and thisBar > 0:

                # data_list = {
                data_list.append({
                    'sCode': sCode,
                    'tBeginDateTime': apex['tBeginDateTime'],
                    'iBeginDif': str(apex['iBeginDif']),
                    'tApexDateTime': str(apex['tApexDateTime']),
                    'iApexDif': str(apex['iApexDif']),
                    'tEndDateTime': tDateTime,
                    'iEndDif': str(thisDif),
                    'iDirectionType': str(2),
                    # }
                })
                # debug.p(pd.Series(data_list))
                # macd_data_apex = macd_data_apex.append(data_list, ignore_index=True)
                # debug.p(macd_data_apex)


                # dataList.append([sCode, apex['tBeginDateTime'], str(apex['iBeginDif']), apex['tApexDateTime'], str(apex['iApexDif']), tDateTime, str(thisDif), str(2)])
                apex['iBeginDif'] = thisDif
                apex['tBeginDateTime'] = tDateTime
                apex['iApexDif'] = thisDif
                apex['tApexDateTime'] = tDateTime
                apex['iDirectionType'] = 1
            # 当 在非下降趋势中,出现当前dif比最低dif低了一个系数则认为是改变成了下降
            elif k < slope * -1 and direction != 2 and thisBar < 0:
                data_list.append({
                    'sCode': sCode,
                    'tBeginDateTime': apex['tBeginDateTime'],
                    'iBeginDif': str(apex['iBeginDif']),
                    'tApexDateTime': str(apex['tApexDateTime']),
                    'iApexDif': str(apex['iApexDif']),
                    'tEndDateTime': tDateTime,
                    'iEndDif': str(thisDif),
                    'iDirectionType': str(1),
                })
                # macd_data_apex = macd_data_apex.append(data_list, ignore_index=True)
                # dataList.append([sCode, apex['tBeginDateTime'], str(apex['iBeginDif']), apex['tApexDateTime'], str(apex['iApexDif']), tDateTime, str(thisDif), str(1)])
                apex['iBeginDif'] = thisDif
                apex['tBeginDateTime'] = tDateTime
                apex['iApexDif'] = thisDif
                apex['tApexDateTime'] = tDateTime
                apex['iDirectionType'] = 2

            # 当 在上升趋势中, 当前dif比最高dif还要高则替换
            if direction == 1 and apex['iApexDif'] <= thisDif:
                apex['iApexDif'] = thisDif
                apex['tApexDateTime'] = tDateTime
            # 当 在下降趋势中, 当前dif比最低dif还要低则替换
            elif direction == 2 and apex['iApexDif'] >= thisDif:
                apex['iApexDif'] = thisDif
                apex['tApexDateTime'] = tDateTime

        # 初始数据
        else:
            apex['iBeginDif'] = thisDif
            apex['tBeginDateTime'] = tDateTime
            apex['iApexDif'] = thisDif
            apex['tApexDateTime'] = tDateTime
            apex['iDirectionType'] = 0  # 方向 0:不确定方向 1:向上 2:向下

        end = time.time()
        totle_time += end - start
    print(totle_time)
    if data_list:
        macd_data_apex = macd_data_apex.append(data_list, ignore_index=True)
    # debug.p(macd_data_apex[[ 'tBeginDateTime', 'tApexDateTime','tEndDateTime', 'iDirectionType']])
    return macd_data_apex
    # for a in dataList:
    #     print(a)
    # exit()


engine = sql_model.get_conn()
sql = "select * from stock_basics order by code asc"
# sql = "select * from stock_basics where code > '601312' order by code asc"
df = pd.read_sql(sql, engine)
code_list = df['code']

macd_data_apex = pd.DataFrame()
for s in code_list:
    print(s + " begin ")
    macd_data_apex = get_mack_apex(s)
    # aField = ['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']
    result = sql_model.loadData('stock_daily_macd_apex', macd_data_apex.keys(), macd_data_apex.values)
    print(s + " end ")

#
# macd_data_apex = get_mack_apex('600077')
# debug.p(macd_data_apex)
# debug.p(macd_data_apex)
# aField = ['sCode', 'tBeginDateTime', 'iBeginDif', 'tApexDateTime', 'iApexDif', 'tEndDateTime', 'iEndDif', 'iDirectionType']
# result = sql_model.loadData('stock_daily_macd_apex', aField, dataList)
# print(result)
