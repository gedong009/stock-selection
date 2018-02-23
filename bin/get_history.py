#encoding: utf-8
import urllib.request
import json
import sql_model


import codecs

# 网址
url = "http://q.stock.sohu.com/hisHq?code=cn_601766,cn_600077&start=19860101&end=20170901"
# url = "http://q.stock.sohu.com/hisHq?code=cn_601766,cn_600077&start=20170701&end=20170705"
# 请求
request = urllib.request.Request(url)
# 爬取结果
response = urllib.request.urlopen(request)

data = response.read()
# 设置解码方式
data = data.decode('utf-8')
# 讲string转成jsonlist
array = json.loads(data)

# 遍历jsonlist
aDataInfo = []
for i in array:
    # 取出股票编码
    code = i['code'][3:9]
    # 遍历股票详情
    for j in i['hq']:
        # 拼装格式为
        # 编号  日期  开盘  收盘  涨跌  涨幅(%)  最低  最高  总手  金额  换手率(%)
        thisDataInfo = [code]
        for k in j:
            thisDataInfo.append(k.replace("%", ""))
        aDataInfo.append(thisDataInfo)
aField = ['sCode', 'tDateTime', 'iOpeningPrice', 'iClosingPrice', 'iTodayPrice', 'iPriceChangeRatio', 'iMinimumPrice', 'iMaximumPrice', 'iVol', 'iTurnover', 'iTurnoverRate']
sql_model.loadData("stock_daily_data", aField, aDataInfo)
print("ok")
