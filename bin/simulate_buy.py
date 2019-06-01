#encoding: utf-8
import pymysql
import sql_model
import pandas as pd
import numpy as np
from debug import p
import debug

def simulate_buy(code):
    loadDataSql = "select * " \
                  "from stock_daily_macd_deviate d " \
                  "left join stock_daily_data sd on sd.sCode = d.sCode " \
                  "where d.iDirectionType=2 " \
                  "and d.sCode='" + code + "' " \
                  "and d.tDeviateDateTime>='2008-01-01' " \
                  "and sd.iOpeningPrice is not null " \
                  "group by d.tDeviateDateTime "
                  # " and tApexDateTime='2017-04-11'"
    print(loadDataSql)
    row_df = pd.read_sql(loadDataSql, engine)
    if len(row_df) < 1:
        return
    # exit()
    # row_array = sql_model.getAll(loadDataSql)

    # for a in row_array:
    #     print(a)
    # exit()
    # 最终结果列表
    dataList = []
    zPrice = 0
    zPercent = 0
    zNum = 0
    fPrice = 0
    fPercent = 0
    fNum = 0
    TotalPercent = 0
    for row in row_df.values:
        sCode = str(row[1])
        tBeginDateTime = str(row[2])
        tDeviateDateTime = str(row[4])

        #确认买入点
        loadDataSql = "select m.tDateTime, d.iOpeningPrice, d.iClosingPrice " \
                      "from stock_daily_macd m " \
                      "left join stock_daily_data d on m.tDateTime=d.tDateTime and m.sCode=d.sCode " \
                      "where m.tDateTime >= '" + tDeviateDateTime +"' " \
                      "and m.iBar > 0 " \
                      "and m.sCode='" + code + "' " \
                      "and d.iOpeningPrice is not null " \
                      "limit 2"
        # row_array = sql_model.getAll(loadDataSql)
        print(loadDataSql)
        buy_df = pd.read_sql(loadDataSql, engine)
        if len(buy_df) < 2:
            continue
        buyDate = buy_df.loc[1, 'tDateTime']
        buyPrice = buy_df.loc[1, 'iOpeningPrice']
        # buyDate = row_array[1][0]
        # buyPrice = row_array[1][1]

        #确认卖出点
        loadDataSql = "select m.tDateTime, d.iOpeningPrice, d.iClosingPrice " \
                      "from stock_daily_macd m " \
                      "left join stock_daily_data d on m.tDateTime=d.tDateTime and m.sCode=d.sCode " \
                      "where m.tDateTime >= '" + str(buyDate) + "' " \
                      "and (m.iBar < 0 or (d.iClosingPrice / " + str(buyPrice) + " - 1) < -0.15 ) " \
                      "and m.sCode='" + code + "' " \
                      "limit 2"
        """
        loadDataSql = "select m.tDateTime, d.iOpeningPrice, d.iClosingPrice " \
                      "from stock_daily_macd m " \
                      "left join stock_daily_data d on m.tDateTime=d.tDateTime and m.sCode=d.sCode " \
                      "where m.tDateTime >= '" + str(buyDate) + "' " \
                      "and (m.iBar < 0 or d.ma60 - d.iClosingPrice < 0) " \
                      "and m.sCode='" + code + "' " \
                      "limit 2"
                      """
        # print(loadDataSql)

        # row_array = sql_model.getAll(loadDataSql)
        sell_df = pd.read_sql(loadDataSql, engine)
        if len(sell_df) > 1:
            sellDate = sell_df.loc[1, 'tDateTime']
            sellPrice = sell_df.loc[1, 'iOpeningPrice']
            # debug.p(sell_df.loc[1,'tDateTime'])
            earnings = round(sellPrice - buyPrice, 2)
            percent = round(((sellPrice / buyPrice) - 1) * 100, 2)
            TotalPercent = round(TotalPercent + percent, 2)
            if earnings > 0:
                zPrice = zPrice + earnings
                zPercent = zPercent + percent
                zNum = zNum+1
            else:
                fPrice = round(fPrice + earnings, 2)
                fPercent = fPercent + percent
                fNum = fNum+1

            # print(buyDate, buyPrice, sellDate, sellPrice, earnings)
            dataList.append([sCode, str(buyDate), str(buyPrice), str(sellDate), str(sellPrice), str(earnings), str(percent)])

    # for a in dataList:
    #     print(a)


    d = {
        '股票代码': code,
        '盈利点数': zPrice,
        '盈利数': float(zNum),
        '平均盈利点数': round(zPrice/zNum, 2) if zNum > 0 else 0,
        '平均盈利百分比': round(zPercent/zNum, 2) if zNum > 0 else 0,
        '亏损点数': fPrice,
        '亏损数': float(fNum),
        '平均亏损点数': round(fPrice/fNum, 2) if fNum > 0 else 0,
        '平均亏损百分比': round(fPercent/zNum, 2) if fNum > 0 else 0,
        '总收益百分比': TotalPercent
    }
# debug.p(d.keys())
    df = pd.DataFrame(data=d, columns=d.keys(), index=[0])
    return df

engine = sql_model.get_conn()
# sql = "select * from stock_basics"
sql = "SELECT DISTINCT sCode FROM stock_daily_macd_deviate order by sCode desc"
df = pd.read_sql(sql, engine)
code_list = df['sCode']

buy_info = pd.DataFrame()
for s in code_list:
    print(s + " begin ")
    p(simulate_buy(s))
    buy_info = buy_info.append(simulate_buy(s), ignore_index=True)
    # aField = ['sCode', 'tDateTime', 'iEmaShort', 'iEmaLong', 'iDif', 'iDea', 'iBar']
    print(s + " end ")

print(buy_info)
buy_value = buy_info.loc[:, ['盈利点数', '盈利数', '平均盈利点数', '平均盈利百分比', '亏损点数', '亏损数', '平均亏损点数', '平均亏损百分比', '总收益百分比']]
print(buy_value.dtypes)
# exit()
buy_total = buy_value.agg(np.cumsum)
print(buy_total)
print(buy_total.iloc[-1] / len(buy_value))

exit()
# df = simulate_buy("603683")
# debug.p(df)
# exit()
# aField = ['sCode', 'tBuyDate', 'iBuyPrice', 'tSellDate', 'iSellPrice', 'iEarnings']
# result = sql_model.loadData('stock_simulate_buy', aField, dataList)
# print(result)
