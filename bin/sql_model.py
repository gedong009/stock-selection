#encoding: utf-8
import os
from sqlalchemy import create_engine
import pymysql
import shutil

def loadData(tableName, aField, params):
    # 定义
    sysPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    fileName = tableName + ".txt"
    dataDirPath = sysPath + "/data"
    dataPath = dataDirPath + fileName
    mysqlFilePath = "/var/lib/mysql-files/" + fileName

    # 创建文件夹
    if os.path.isdir(dataDirPath) == False:
        os.mkdir(dataDirPath, 755)

    file_object = open(dataPath, 'w')

    # 遍历jsonlist
    for i in params:
        # 根据分隔符\t分割列表
        i = [str(j) for j in i]
        row = "\t".join(i) + "\n"
        # 写入内容
        file_object.write(row)
    # 关闭文件
    file_object.close()
    # 把文件复制到mysql可执行的目录下去 可通过show variables like '%secure%';查看
    shutil.copyfile(dataPath, mysqlFilePath)

    # 打开数据库连接
    conn = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock', charset='utf8')
    # 创建游标
    cursor = conn.cursor()
    # 执行SQL，并返回收影响行数
    # loadDataSql = "load data local infile '" + mysqlFilePath + "' ignore into table stock_daily_data;"
    # loadDataSql = "load data infile '" + mysqlFilePath + "' ignore into table " + tableName + "(" + ",".join(aField) + ");"
    loadDataSql = "load data infile '" + mysqlFilePath + "' replace into table " + tableName + "(" + ",".join(aField) + ");"
    print(loadDataSql)

    effect_row = cursor.execute(loadDataSql)
    # 关闭游标
    cursor.close()
    # 确认
    conn.commit()
    # 关闭连接
    conn.close()
    # 获取剩余结果的第一行数据
    # row_1 = cursor.fetchone()

    return effect_row

def getAll(loadDataSql):
    conn = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock', charset='utf8')
    cursor = conn.cursor()
    effect_row = cursor.execute(loadDataSql)
    row_array = cursor.fetchall()
    cursor.close()
    conn.close()
    return row_array

def getOne(loadDataSql):
    conn = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock', charset='utf8')
    cursor = conn.cursor()
    effect_row = cursor.execute(loadDataSql)
    row_array = cursor.fetchone()
    cursor.close()
    conn.close()
    return row_array


# new
def get_conn():
    conn = create_engine('mysql://root:gedongSql@123@112.124.4.247/stock?charset=utf8')
    # conn = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock', charset='utf8')
    return conn
