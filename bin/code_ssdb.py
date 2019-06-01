import sql_model
import pandas as pd
import pyssdb

key = "get_h_hfq_code_list"


# 获取ssdb客户端
def ssdb_client():
    c = pyssdb.Client("112.124.4.247", 8888)
    return c


# 重置股票代码列表
def reset_codelist_ssdb(id=None):
    c = ssdb_client()
    if id:
        _key = key + "_" + id
    else:
        _key = key

    c.qclear(_key)
    engine = sql_model.get_conn()

    sql = "select * from stock_basics order by code asc"
    df = pd.read_sql(sql, engine)
    code_list = df['code']
    num = 0
    for s in code_list:
        c.qpush_back(_key, s)
        # print(s)
        num += 1
    print(num)


# 获取下一个股票代码
def get_next_code(id=None):
    if id:
        _key = key + "_" + id
    else:
        _key = key
    c = ssdb_client()
    value = c.qpop_front(_key)
    if value:
        value = str(value, encoding='utf-8')
    return value

# while 1 == 1:
#
#     code = get_next_code()
#     if code:
#         print(code)
#     else:
#         break
# # for s in code:
# #     print(s)
# # c = ssdb_client()
# print(reset_codelist_ssdb())
# exit()
# reset_codelist_ssdb()
