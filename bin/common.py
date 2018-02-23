#encoding: utf-8
import sql_model
import pandas as pd
import debug
import time
import pymysql
import os
import datetime
from sqlalchemy import create_engine


def file_write(name, params):

    # 定义
    sys_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    file_name = name + ".txt"
    data_dir_path = sys_path + "/h_log/"
    data_path = data_dir_path + file_name
    # mysqlFilePath = "/var/lib/mysql-files/" + file_name

    # 创建文件夹
    if os.path.isdir(data_dir_path) == False:
        os.mkdir(data_dir_path, 755)

    file_object = open(data_path, 'a')

    if isinstance(params, str):
        params = [params]
    if not isinstance(params, list) and not isinstance(params, tuple) and not isinstance(params, dict):
        return False

    # 遍历jsonlist
    file_object.write(str(datetime.datetime.now()))
    for i in params:
        # 根据分隔符\t分割列表
        row = "\t" + str(i)
        # 写入内容
        file_object.write(row)
    # 关闭文件
    file_object.write("\n")
    file_object.close()


# 拿出5天内的最低点
def getWithinDaysKLineApex(sCode, tDateTime, iDirectionType = 2, days = 5):
    getDays = days - 1;
    if getDays % 2 == 0:
        beforeNum = int(getDays / 2)
    else:
        beforeNum = getDays % 2
    if iDirectionType == 2:
        orderBySql = "iMinimumPrice asc"
    else :
        orderBySql = "iMaximumPrice desc"
    loadDataSql = "SELECT * FROM (" \
                      "SELECT * FROM stock_daily_data WHERE tDateTime >= (" \
                          "SELECT tDateTime FROM (" \
                                "SELECT * FROM stock_daily_data WHERE tDateTime < '" + tDateTime + "' AND sCode = '" + sCode + "' ORDER BY tDateTime DESC LIMIT " + str(beforeNum) + " " \
                          ") AS before2 ORDER BY before2.tDateTime LIMIT 1" \
                      ") AND sCode = '" + sCode + "' ORDER BY	tDateTime LIMIT " + str(days) + " " \
                  ") AS five ORDER BY " + orderBySql + " LIMIT 1"
    # print(loadDataSql)
    apexData = sql_model.getOne(loadDataSql)
    return apexData


# 根据时间段找K线顶点
def getKLineApexByTime(sCode, sStartTime, sEndTime, iDirectionType = 2):
    if iDirectionType == 2:
        orderBySql = "iMinimumPrice asc"
    else :
        orderBySql = "iMaximumPrice desc"
    loadDataSql = "SELECT * FROM stock_daily_data WHERE tDateTime >= '" + str(sStartTime) + "' AND tDateTime <= '" + str(sEndTime) + "' AND sCode = '" + sCode + "' " \
                  "ORDER BY " + orderBySql + ", tDateTime desc LIMIT 1"
    # print(loadDataSql)
    apexData = sql_model.getOne(loadDataSql)
    return apexData

# 往前找{days}天, 返回顶点日期
def getBeforDaysKLineApex(sCode, tDateTime, iDirectionType = 2, days = 40):
    if iDirectionType == 2:
        orderBySql = "iMinimumPrice asc"
    else :
        orderBySql = "iMaximumPrice desc"
    loadDataSql = "SELECT * FROM (" \
                    "SELECT * FROM stock_daily_data WHERE tDateTime < '" + tDateTime + "' AND sCode = '" + sCode + "' ORDER BY tDateTime DESC LIMIT " + str(days) + " " \
                  ") AS beforeDays ORDER BY " + orderBySql + " LIMIT 1"
    apexData = sql_model.getOne(loadDataSql)
    # print(loadDataSql)
    return apexData


# 排除早期dif值较低的情况(顶背离相反)
def getBeforeMacdApex(sCode, tDateTime, iThisApexDif, iApexPrice, iDirectionType = 2, count = 10):
    if iDirectionType == 2:
        # orderBySql = "iApexDif asc"
        whereApexDifSql = "iApexDif < " + str(iThisApexDif)
        whereApexPriceSql = "beforeData.iMinimumPrice > " + str(iApexPrice)
    else :
        # orderBySql = "iApexDif desc"
        whereApexDifSql = "iApexDif > " + iThisApexDif
        whereApexPriceSql = "beforeData.iMaximumPrice < " + str(iApexPrice)
    loadDataSql = "select tApexDateTime, iApexDif, iMinimumPrice, iMaximumPrice from (" \
                      "SELECT tApexDateTime, iApexDif, iMinimumPrice, iMaximumPrice FROM stock_daily_macd_apex a " \
                      "left join stock_daily_data d on a.sCode = d.sCode and a.tApexDateTime = d.tDateTime " \
                      "where a.sCode = '" + sCode + "' " \
                          "and a.iDirectionType = " + str(iDirectionType) + " " \
                          "and a.tApexDateTime < '" + tDateTime + "' " \
                      "ORDER BY a.tApexDateTime desc " \
                      "limit " + str(count) + " " \
                  ") as beforeData " \
                  "where " + whereApexDifSql + " " \
                      "and " + whereApexPriceSql + " " \
                  "order by tApexDateTime desc"
    # print(loadDataSql)
    # exit()
    beforMacdApex = sql_model.getAll(loadDataSql)
    return beforMacdApex


def strtotime(sDateTime):
    return time.mktime(time.strptime(str(sDateTime) + " 00:00:00", "%Y-%m-%d %H:%M:%S"))


# 获取所有股票列表
def get_stock_basics_list():
    engine = sql_model.get_conn()
    sql = "select * from stock_basics"
    df = pd.read_sql(sql, engine)
    return df


# 获取移动均线
def get_average_line(code, stock_data=None):
    if stock_data is None:
        # 打开数据库连接
        # engine = sql_model.get_conn()
        engine = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock', charset='utf8')
        sql = "select * from stock_daily_data where sCode='" + code + "' order by tDateTime asc"
        stock_data = pd.read_sql(sql, engine)
        stock_data.index = pd.DatetimeIndex(stock_data.tDateTime)
    # 均线周期
    ma_list = [5, 10, 20, 30, 60, 120]
    for ma in ma_list:
        stock_data['ma' + str(ma)] = round(stock_data['iClosingPrice'].rolling(ma).mean(), 4)
    return stock_data

