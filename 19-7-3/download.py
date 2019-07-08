import pandas as pd
import requests
import json


# 设置打印展示输出
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 10000)

# df1 = pd.read_excel("/Users/apple/Documents/project/analyze/19-7-3/zqh6月2号-30号还款新用户运营商.xlsx")
# df1 = df1.drop_duplicates(subset=["mobile"])
# df2 = pd.read_csv("/Users/apple/Documents/project/analyze/19-7-3/zqh6月2号-30号还款新用户通讯录.csv")
# df2 = df2.drop_duplicates(subset=["mobile"])
# df2 = df2.drop(columns=["name","idcard"])
#
# df3_ = pd.read_csv("zqh6月2号-30号还款新用户还款标签.csv")
# df3 = pd.DataFrame()
# df3["mobile"] = df3_["phone"]
# df3["repay"] = df3_["repay"].astype('int')
# df3 = df3.groupby("mobile").max()
# df3 = df3.reset_index()
#
# df = df1.merge(df2,on="mobile",how="inner").merge(df3,on="mobile",how="inner")
df = pd.read_csv("url_label.csv")

def phone_handle(x):
    x = x.replace("+86","")
    x = x.replace(" ","")
    return x

for index,item in enumerate(df.itertuples(index=False)):
    mobile = str(item[0])
    result = []
    print(f"{index}/8937 ",f"手机号{mobile}")
    result.append(mobile)

    # 通讯录，下载并加载成df，转换成series
    resp = requests.get(item[2])
    resp.encoding = "utf-8"
    contacts_text = resp.text
    contacts_json = json.loads(contacts_text)
    df_contacts = pd.DataFrame(contacts_json)
    df_contacts["mobile"] = df_contacts["mobile"].map(phone_handle)
    # 获取通讯录去重的号码，手机号码的series
    sl_contacts = df_contacts['mobile'].drop_duplicates()
    sl_contacts_mobile = sl_contacts[sl_contacts.str.match(r"^1[3456789]\d{9}$")]
    mobile_rate = sl_contacts_mobile.shape[0]/sl_contacts.shape[0]
    result.append(mobile_rate)

    # 通话记录，下载并加载成df，转换成series
    resp= requests.get(item[1])
    resp.encoding = "utf-8"
    callRecord_text = resp.text
    callRecord_json = json.loads(callRecord_text)
    modify_time = pd.to_datetime(callRecord_json["last_modify_time"])
    callRecord_con = callRecord_json["calls"]
    callRecord = []
    for callRecord_month in callRecord_con:
        callRecord.extend(callRecord_month.get("items",[]))
    df_callRecord = pd.DataFrame(callRecord)
    df_callRecord.rename(columns={'peer_number': 'mobile'}, inplace=True)
    df_callRecord["time"] = pd.to_datetime(df_callRecord["time"])
    # 获取通话详单1m，6m的去重号码
    sl_callRecord_1m = df_callRecord[(modify_time-df_callRecord["time"]).dt.days<=30]["mobile"].drop_duplicates()
    sl_callRecord_6m = df_callRecord["mobile"].drop_duplicates()

     # 通话详单去重的号码  和 通讯录的号码， 交集，  求个数
    for sl_callRecord in [sl_callRecord_1m,sl_callRecord_6m]:
        for sl_contact in [sl_contacts,sl_contacts_mobile]:
            sl_con = sl_callRecord.append(sl_contact)
            sl = sl_con[sl_con.duplicated()]
            result.append(sl.shape[0])

    # 添加标签
    result.append(item[3])
    print(result,"\n")

    # 存储结果
    result_line = ','.join(list(map(lambda x:str(x),result))) + "\n"
    with open("dataset/result.csv","a",encoding="utf-8") as f1:
        f1.write(result_line)

    # 缓存下载的文件
    local_file_line = "^".join([mobile,contacts_text,callRecord_text]) + '\n'
    with open("dataset/local_storage.txt","a",encoding="utf-8") as f2:
        f2.write(local_file_line)


