import pandas as pd

from tools.odpsObj import obj
from tools.auto_binning import auto_bin




df1 = pd.read_excel("label.xlsx")

# 按照新用户的标签
# df1 = df1[df1["user_type"]==0]
# df1['repay_time'] = pd.to_datetime(df1['repay_time'])
# df1['real_repay_time'] = pd.to_datetime(df1['real_repay_time'].dt.strftime('%Y-%m-%d'))
# overdue_df = pd.DataFrame()
# overdue_df['mobile'] = df1['phone']
# overdue_df['label'] = (df1['real_repay_time'].isna() | (df1['real_repay_time'] > df1['repay_time'])).map(lambda x:1 if x else 0)
# overdue_df = overdue_df.drop_duplicates(subset=['mobile'])

# 按照出现了逾期就为逾期的标签
df1['repay_time'] = pd.to_datetime(df1['repay_time'])
df1['real_repay_time'] = pd.to_datetime(df1['real_repay_time'].dt.strftime('%Y-%m-%d'))
overdue_df = pd.DataFrame()
overdue_df['mobile'] = df1['phone']
overdue_df['label'] = (df1['real_repay_time'].isna() | (df1['real_repay_time'] > df1['repay_time'])).map(lambda x:1 if x else 0)
overdue_df = overdue_df.groupby("mobile").max().reset_index()

df2 = pd.read_json('trip_record.txt',lines=True)
df2 = df2.drop_duplicates(subset=['mobile'])
df2.to_csv("trip_record.csv",index=False)
# obj.uploadCSV("trip_record.csv","trip_record")


df = df2.merge(overdue_df,on='mobile')

cols = list(df.columns)
cols.remove("mobile")
cols.remove("last_modify_time")
cols.remove("label")

auto_bin(df,'label',cols,"trip_record_yuqi","出现了逾期")



