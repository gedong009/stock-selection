import tushare as ts
import sql_model
from debug import p

# 行业分类
df = ts.get_industry_classified()
df.rename(columns={'c_name': 'sIndustryName', 'name': 'sName', 'code': 'sCode'}, inplace=True)
result = sql_model.loadData('stock_sector_industry', df.keys(), df.values)
print("行业: %s" % result)

# 概念分类
df = ts.get_concept_classified()
df.rename(columns={'c_name': 'sConceptName', 'name': 'sName', 'code': 'sCode'}, inplace=True)
result = sql_model.loadData('stock_sector_concept', df.keys(), df.values)
print("概念: %s" % result)

# 地域分类
df = ts.get_area_classified()
df.rename(columns={'area': 'sAreaName', 'name': 'sName', 'code': 'sCode'}, inplace=True)
result = sql_model.loadData('stock_sector_area', df.keys(), df.values)
print("地域: %s" % result)

