#encoding: utf-8
import os
from sqlalchemy import create_engine
import pymysql
import pymysql_manager
import shutil
from debug import p

pymysql_manager.ConnectionPooled()
# mysql链接
mysql_conn = None
# 是否是自定义链接
is_custom_conn = False


class MysqlClass:

    # 当conn传入值时， 认为是自定义的mysql链接，不会自动关闭链接
    # 无conn传入，则自动创建链接并当操作完毕后就关闭连接
    def __init__(self, conn=None):
        global mysql_conn
        global is_custom_conn
        if conn is None:
            mysql_conn = self.create_conn()
            is_custom_conn = False
        else:
            mysql_conn = conn
            is_custom_conn = True

    # 创建链接
    def create_conn(self):
        # conn = pymysql.connect(host='112.124.4.247', port=3306, user='root', passwd='gedongSql@123', db='stock',
        #                        charset='utf8')
        conn = pymysql_manager.ConnectionPooled(host='112.124.4.247', database='stock', user='root', passwd='gedongSql@123',
                              pool_options=dict(max_size=10, max_usage=100000, idle=60, ttl=120))
        print("create")
        # sql = "select * from stock_daily_big_order"
        # with db.pool() as connection:
        #     effect_row = connection.execute(sql)
        #     p(effect_row)
        # p("sdf")
        return conn

    # 关闭连接
    def close_conn(self):
        print("close")
        global mysql_conn
        mysql_conn.close()

    def loadData(self, tableName, aField, params, thread=1):
        global mysql_conn
        global is_custom_conn
        # 定义
        sysPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        fileName = tableName + "_" + str(thread) + ".txt"
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
        global mysql_conn

        # 创建游标
        cursor = mysql_conn.cursor()
        # 执行SQL，并返回收影响行数
        # loadDataSql = "load data local infile '" + mysqlFilePath + "' ignore into table stock_daily_data;"
        # loadDataSql = "load data infile '" + mysqlFilePath + "' ignore into table " + tableName + "(" + ",".join(aField) + ");"
        loadDataSql = "load data infile '" + mysqlFilePath + "' replace into table " + tableName + "(" + ",".join(aField) + ");"
        print(loadDataSql)
        print(cursor)

        with mysql_conn.pool() as connection:
            effect_row = connection.execute(loadDataSql)

        # effect_row = cursor.execute(loadDataSql)
        # # 关闭游标
        # cursor.close()
        # # 确认
        # mysql_conn.commit()
        # # 关闭连接
        # if is_custom_conn == True:
        #     mysql_conn.close()
        # # 获取剩余结果的第一行数据
        # # row_1 = cursor.fetchone()

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
