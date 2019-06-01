import sql_model
import pandas as pd
import redis
from debug import p

key = "get_h_hfq_code_list"


# 获取redis客户端
def redis_client():
    r = redis.Redis(host='112.124.4.247', port=6379, db=0)
    return r


# 重置股票代码列表
def reset_codelist_redis(id=None):

    c = redis_client()
    if id:
        _key = key + "_" + id
    else:
        _key = key
    c.delete(_key)
    engine = sql_model.get_conn()
    sql = "select * from stock_basics order by code asc"
    # sql = "select * from stock_basics where code > '600000' order by code asc"

    df = pd.read_sql(sql, engine)
    code_list = df['code']
    num = 0
    for s in code_list:
        c.lpush(_key, s)
        # print(s)
        num += 1
    print(_key)
    return num
    print(num)


# 获取下一个股票代码
def get_next_code(id=None):
    if id:
        _key = key + "_" + id
    else:
        _key = key
    c = redis_client()
    value = c.rpop(_key)
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
# # c = redis_client()
# print(reset_codelist_redis())
# exit()
# reset_codelist_redis()
