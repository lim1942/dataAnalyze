import pandas as pd
# 设置打印展示输出
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)

import sys
sys.path.append("/Users/apple/Documents/project/analyze/tools")
from binning import binning_sparse_col,binning_cate,binning_num,binning_aeq


# def handle(x):
#     if not x:
#         return x
#     return x.split("_")[0]
#
# df = pd.read_json("new/strategy_judge.txt",lines=True)
# cols = ["callRecord_7day_cnt", "callRecord_1m_cnt", "callRecord_3m_cnt", "callRecord_6m_cnt","over_location_cnt_7day", "over_location_cnt_1m", "over_location_cnt_2m", "over_location_cnt_3m", "over_location_cnt_6m",]
# for col in cols:
#     df[col] = df[col].map(handle).fillna(-1)
#     df[col] = df[col].astype("int")
#     bindf = binning_sparse_col(df,"label",col,max_bin=10,sparse_value=-1)
#     bindf_bad_rate_max = bindf["bad_rate"].max()
#     if bindf_bad_rate_max >0.50:
#         _range = bindf[bindf["bad_rate"]==bindf_bad_rate_max][col].values[0]
#         df[col] = df[col].map(lambda x:0 if x in _range else 1)
#         print(bindf_bad_rate_max,col,_range)
#     else:
#         df.drop(columns=[col],inplace=True)
#         print("drop",col)
# mobile = df['mobile']
# df.to_csv("new/strategy_judge_convert.csv",index=False)
df =pd.read_csv("new/strategy_judge_convert.csv")
df = df.drop(columns=["mobile"])
print(df.columns)