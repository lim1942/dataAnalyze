import json
import pandas as pd

import sys
sys.path.append("/Users/apple/Documents/project/analyze/tools")
from binning import binning_sparse_col,binning_cate,binning_num,binning_aeq

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)


df1 = pd.read_excel("副本魔蝎result.xlsx")
df1 = df1.drop_duplicates(subset="phone",keep=False)
df2 = pd.read_csv("/Users/apple/Documents/project/analyze/blackList/测试样本_1逾期_0未逾期.csv")
df = df1.merge(df2,how="inner",on="phone")

df["time"] = pd.to_datetime(df["time"])
df.index = df["time"]
# df = df.to_period(freq="H")
# print(df.sort_index())
print(df['2019-06-11 23H'])
# def handle(x):
#     x = json.loads(x)["data"]["attention_info"]
#     idcard_name_in_attentionlist2 = x["idcard_name_in_attentionlist2"]
#     mobile_name_in_attentionlist2 = x["mobile_name_in_attentionlist2"]
#     idcard_name_in_attentionlist1 = x["idcard_name_in_attentionlist1"]
#     mobile_name_in_attentionlist1 = x["mobile_name_in_attentionlist1"]
#     return idcard_name_in_attentionlist2 +"^"+ mobile_name_in_attentionlist2 +"^"+ \
#            idcard_name_in_attentionlist1 + "^"+ mobile_name_in_attentionlist1
# df["statue"] = df["statue"].map(handle)
# df["idcard_name_in_attentionlist2"] = df["statue"].str.split("^").str[0]
# df["mobile_name_in_attentionlist2"] = df["statue"].str.split("^").str[1]
# df["idcard_name_in_attentionlist1"] = df["statue"].str.split("^").str[2]
# df["mobile_name_in_attentionlist1"] = df["statue"].str.split("^").str[3]
# for i in ["idcard_name_in_attentionlist2","mobile_name_in_attentionlist2",
#           "idcard_name_in_attentionlist1","mobile_name_in_attentionlist1"]:
#     print(binning_cate(df,"repay",i))

# print(df["statue"].value_counts())
# df_no = df[df["statue"]==0]
# print("无命中")
# print("逾期人数：",df_no["repay"].sum())
# print("总人数：",len(df_no))
# print("逾期率：",df_no["repay"].sum()/len(df_no),"\n")
# df_yes = df[df["statue"]==1]
# print("命中1次")
# print("逾期人数：",df_yes["repay"].sum())
# print("总人数：",len(df_yes))
# print("逾期率：",df_yes["repay"].sum()/len(df_yes),"\n")
# df_yes = df[df["statue"]==2]
# print("命中2次")
# print("逾期人数：",df_yes["repay"].sum())
# print("总人数：",len(df_yes))
# print("逾期率：",df_yes["repay"].sum()/len(df_yes),"\n")
# df_yes = df[df["statue"]==3]
# print("命中3次")
# print("逾期人数：",df_yes["repay"].sum())
# print("总人数：",len(df_yes))
# print("逾期率：",df_yes["repay"].sum()/len(df_yes))
