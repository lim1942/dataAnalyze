import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import sys
sys.path.append("/Users/apple/Documents/project/analyze/tools")
from binning import binning_sparse_col,binning_cate,binning_num,binning_aeq
# 设置打印展示输出
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)

# label 数据
label = pd.read_csv("label")
label.drop_duplicates(subset=["身份证号"],keep=False,inplace=True)



# # # 1 综合决策报告
# df = pd.read_excel("天鹂-风险评估、欺诈甄别-小额、小额分-科技版-测试明细.xlsx","综合决策报告")
# df["综合评分(小额)"] = df["综合评分(小额)"].map(lambda x:-1 if x=="资料不足，无法评分" else x)
# df.drop_duplicates(subset=["身份证号"],keep=False,inplace=True)
# df = df.merge(label,how="inner",on="身份证号")
# # # 综合评分(小额)
# df["综合评分(小额)"] = df["综合评分(小额)"].astype("int")
# print(binning_sparse_col(df,"repay","综合评分(小额)",max_bin=10))
# # # 决策等级
# print(binning_cate(df,"repay","决策等级"))


# # 2 风险评估-借款记录
# df = pd.read_excel("天鹂-风险评估、欺诈甄别-小额、小额分-科技版-测试明细.xlsx","风险评估-借款记录")
# df = df.merge(label,how="inner",on="身份证号")
# def group_hand(group):
#     return pd.Series({
#         "reject_rate":group[group["审批结果"]=="拒贷"].shape[0]/group.shape[0],
#         "pass_rate":group[group["审批结果"]=="批贷已放款"].shape[0]/group.shape[0],
#         "quit_rate":group[group["审批结果"]=="客户放弃"].shape[0]/group.shape[0],
#         "not_repay_count":group[group["审批结果"]=="批贷已放款"].shape[0] - group[group["还款状态"]=="结清"].shape[0]- group[group["还款状态"]=="正常"].shape[0],
#         "repay_rate": (group[group["还款状态"]=="结清"].shape[0]+group[group["还款状态"]=="正常"].shape[0])/group[group["审批结果"]=="批贷已放款"].shape[0] if group[group["审批结果"]=="批贷已放款"].shape[0] else -1,
#         "loan_count":group.shape[0],
#         "repay":group["repay"].mode()[0],
#     })
# df = df.groupby("身份证号").apply(group_hand)
# 5分箱
# for i in ["reject_rate","pass_rate","quit_rate","not_repay_count"]:
#     print(binning_num(df,"repay",i,5))
# # 10分箱
# print(binning_num(df,"repay","loan_count",10))
# 特殊值的分箱
# print(binning_sparse_col(df,"repay","repay_rate",max_bin=5))


# # 3 风险评估 - 多头查询记录
# df = pd.read_excel("天鹂-风险评估、欺诈甄别-小额、小额分-科技版-测试明细.xlsx","风险评估-多头查询记录")
# df = df.drop_duplicates(subset=["身份证号"])
# df = df.merge(label,how="inner",on="身份证号")
# # 总查询次数
# print(binning_num(df,"repay","总查询次数",11))
# # 查询机构数
# print(binning_num(df,"repay","查询机构数",11))


# # 4 欺诈甄别
# df = pd.read_excel("天鹂-风险评估、欺诈甄别-小额、小额分-科技版-测试明细.xlsx","欺诈甄别-欺诈评分、欺诈等级")
# df = df.drop_duplicates(subset=["身份证号"])
# df = df.merge(label,how="inner",on="身份证号")
# # 欺诈评分
# df["欺诈评分"] = df["欺诈评分"].fillna("-1")
# df["欺诈评分"] = df["欺诈评分"].astype("int")
# print(binning_sparse_col(df,"repay","欺诈评分",max_bin=10))
# # 欺诈等级
# df["欺诈等级"] = df["欺诈等级"].fillna("无")
# print(binning_cate(df,"repay","欺诈等级"))


# # 5风险名单
df = pd.read_excel("天鹂-风险评估、欺诈甄别-小额、小额分-科技版-测试明细.xlsx","欺诈甄别-风险名单")
df = df.merge(label,how="inner",on="身份证号")
def handle(group):
    return pd.Series({
        "bad_count_":group[~group["风险明细"].isna()].shape[0],
        "count_":group.shape[0],
        "bad_rate_":group[~group["风险明细"].isna()].shape[0]/group.shape[0],
        "repay":group["repay"].mode()[0]
    })
new_df = df.groupby("身份证号").apply(handle)
for i in ["bad_count_","count_","bad_rate_"]:
    print(binning_cate(new_df,"repay",i))








