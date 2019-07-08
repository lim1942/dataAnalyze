import numpy as np
import pandas as pd
import scipy.stats as ss
import matplotlib.pyplot as plt
import seaborn as sns


# 设置打印展示输出
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)

# 数据读取合并
df1 = pd.read_excel("分析授信时间样本.xlsx")
df2 = pd.read_excel("分析授信时间样本.xlsx",1)
df2 = df2[~df2["手机号码"].duplicated(keep="first")]
df = df1.merge(df2,on='手机号码',how='inner')


# 字段格式转换
df["注册时间"] = pd.to_datetime(df["注册时间"])
df["申请时间"] = pd.to_datetime(df["申请时间"])
df["到期时间"] = pd.to_datetime(df["到期时间"])
df["还款时间"] = pd.to_datetime(df["还款时间"])
df["授信时间"] = pd.to_datetime(df["授信时间"])

# 删选逾期的数据

df["apply_register"] = (df["申请时间"] - df["注册时间"]).dt.days
df["auth_register"] = (df["授信时间"] - df["注册时间"]).dt.days
df["apply_auth"] = (df["申请时间"] - df["授信时间"]).dt.days
df["label"] = (df["还款时间"].isna()) | ((df["还款时间"]-df["到期时间"]).dt.days>=1)
df["label"] = df["label"].map(lambda x:1 if x else 0)
df= df[df["是否老用户"]==1]
df = df[~df["手机号码"].duplicated(keep='first')]
df = df[df["apply_auth"]>=0]
new_df = pd.DataFrame()
new_df["apply_register"] = df["apply_register"]
new_df["auth_register"] = df["auth_register"]
new_df["apply_auth"] = df["apply_auth"]
new_df["label"] = df["label"]
new_df.to_csv("new_df.csv",index=False)
print(new_df.shape)

# 连续值相关分析 使用相关系数
# 相关性分析，找出对left影响最大的特征
# sns.set_context(font_scale=1.5)
# sns.heatmap(new_df.corr(method='spearman'),vmin=-1,vmax=1,cmap=sns.color_palette("RdBu",n_colors=128))
# plt.show()


# 按照apply_register

new_df["apply_register"] =new_df["apply_register"].map(lambda x:"9+" if x>9 else x)
new_df["apply_auth"] =new_df["apply_auth"].map(lambda x:"9+" if x>9 else x)



f = plt.figure()

# 图一
f.add_subplot(2,2,1)
def group1(group):
    return pd.Series({"rate":group["label"].sum()/group.shape[0],
                      "sum":group.shape[0]})
apply_register = new_df.groupby("apply_register").apply(group1)
sns.pointplot(apply_register.index,apply_register["rate"],)
# 设置xy轴的标签
# plt.xlabel("申请时间-注册时间")
# plt.ylabel("逾期比例")
# 设置点的值
for a, b in zip(range(len(apply_register.index)),apply_register["rate"]):
    print(a,b)
    plt.text(a, b, round(b,2), ha='center', va='bottom', fontsize=10)



f.add_subplot(2,2,2)
sns.barplot(apply_register.index,apply_register["sum"])



f.add_subplot(2,2,3)
def group2(group):
    return pd.Series({"rate": group["label"].sum() / group.shape[0],
                      "sum": group.shape[0]})
apply_auth = new_df.groupby("apply_auth").apply(group2)
sns.pointplot(apply_auth.index,apply_auth["rate"])



f.add_subplot(2,2,4)
sns.barplot(apply_auth.index,apply_auth["sum"])


plt.show()