import pandas as pd
# 设置打印展示输出
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)

import sys
sys.path.append("/Users/apple/Documents/project/analyze/tools")
from binning import binning_sparse_col,binning_cate,binning_num,binning_aeq


def handle(x):
    if not x:
        return x
    return x.split("_")[0]

df = pd.read_json('new/strategy_judge_no_label.csv',lines=True)
cols1= ["callRecord_1m_cnt","callRecord_3m_cnt","over_location_cnt_2m","over_location_cnt_3m","over_location_cnt_6m"]
for col in cols1:
    df.drop(columns=[col], inplace=True)

cols2 = ["callRecord_7day_cnt","callRecord_6m_cnt","over_location_cnt_7day","over_location_cnt_1m"]
for col ,_range in zip(cols2,[pd.RangeIndex(-1,2),pd.RangeIndex(313,331),pd.RangeIndex(-1,3),pd.RangeIndex(460,479)]):
    df[col] = df[col].map(lambda x: 0 if x in _range else 1)

df.to_csv("new/lastResutl_no_label_convert.csv",index=False)




