# encoding: utf-8
import sql_model
import pandas as pd
import common
import debug
import numpy as np

stock_data = common.get_average_line('600077')
stock_data = stock_data.fillna(value=0)
# df = stock_data.loc[:, ['sCode', 'tDateTime', 'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120']]
df = stock_data.astype(np.str)
# debug.p(df.dtypes)
dataList = df.values
aField = ['id', 'sCode', 'tDateTime', 'iOpeningPrice', 'iClosingPrice', 'iTodayPrice',
          'iPriceChangeRatio', 'iMinimumPrice', 'iMaximumPrice', 'iVolume', 'iAmount', 'iTurnoverRate',
          'ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120']
result = sql_model.loadData('stock_daily_data', aField, dataList)
# engine = mysql_model.get_conn()
# stock_data.to_sql('stock_daily_data', engine, if_exists='append', index=False)

